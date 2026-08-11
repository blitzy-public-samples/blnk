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
// component a deployment declares in front of its Kafka brokers (SEC-01, requirement R-7).
//
// # The gap this closes
//
// A subscriber whose registry row records a partition_key_prefix is entitled only to records
// whose message key carries that prefix. Kafka cannot express that: its authorizer authorises
// OPERATIONS ON RESOURCES — topics, groups, the cluster, transactional ids — and a record key
// is not a resource. No arrangement of ACLs confines a consumer to a slice of a topic, and per
// tenant topics, the only Kafka-native alternative, are excluded by the requirement.
//
// Blnk's answer is to withhold topic Read from such a principal entirely — the broker then
// refuses every direct fetch — and to route its records through a component the DEPLOYMENT
// operates, which applies the recorded prefix before returning anything. That component is not
// Blnk's to ship: it is a custom Kafka authorizer plugin or a protocol-aware proxy, a separate
// artefact with its own lifecycle.
//
// WHAT WAS WRONG IS THAT THE COMPONENT WAS TAKEN ON TRUST. Two configuration values — a mode
// and a bootstrap list — were the entire basis for believing it existed and applied the right
// prefix. A deployment could name any distinct address and Blnk would mint a credential, tell
// the subscriber its key scope was enforced at a gateway, and be wrong. That is the same false
// assurance as the client-side-filter reading it replaced, moved one layer out: the credential
// holder was no longer being asked to police itself, but nobody was being asked to prove that
// anybody else was.
//
// So the declaration is now VERIFIED. Before a secret exists and before the broker is touched,
// Blnk calls the component's control endpoint over an authenticated channel and requires it to
// confirm three facts:
//
//  1. it enforces key scopes at all;
//  2. it will do so for THIS principal; and
//  3. the prefix it holds for that principal is BYTE-FOR-BYTE the prefix the registry recorded.
//
// Anything else — unreachable, unauthorised, a mismatched prefix, a malformed body, an explicit
// refusal — and issuance fails with SUBSCRIBER_KEY_SCOPE_UNATTESTED. Blnk will not mint a
// credential describing a boundary it could not get an answer about.
//
// # What this file is NOT
//
// It is not a data plane. Blnk does not read, filter or serve records, and nothing here
// inspects a message. The component's own filtering correctness remains the component's
// responsibility — what Blnk verifies is that a component ANSWERS, that it authenticates Blnk,
// that it claims enforcement for the exact principal and prefix at issue, and that it is told
// when a binding is withdrawn. That is the whole of what an integrating service can establish
// about a peer, and it is strictly more than a configuration string.
//
// # The wire contract, published so a component can implement it
//
// One endpoint, two methods, JSON both ways, `Authorization: Bearer <token>` on both:
//
//	POST <KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_URL>
//	  {"principal":"blnk-sub-acme","subscriber_id":"acme","partition_key_prefix":"ldg_9f1c",
//	   "authorized_topics":["blnk.transactions"],"consumer_group_prefix":"blnk-sub-acme."}
//	→ 200 {"key_scope_enforced":true,"principal":"blnk-sub-acme","partition_key_prefix":"ldg_9f1c"}
//
//	DELETE <KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_URL>?principal=blnk-sub-acme
//	→ 200 or 204, body ignored
//
//	GET <KAFKA_KEY_SCOPE_GATEWAY_ATTESTATION_URL>
//	→ 200 {"key_scope_enforced":true}   (health only; no principal is implied)
//
// The POST is idempotent by principal: it both REGISTERS the binding and attests it, because a
// component cannot honestly attest a prefix it has not been told about, and splitting the two
// into separate calls would leave a window in which Blnk had a confirmation for a binding the
// component had since dropped.
//
// docs/kafka-operations.md carries the same contract in prose, together with the operator
// runbook for standing such a component up.
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
//
// The body carries three short fields, so a kilobyte is generous. It is capped because the peer
// is an operator-supplied component reached over the network: an unbounded read on a
// misbehaving or hostile endpoint is an unbounded allocation inside credential issuance, and
// the refusal that follows a truncated body is the same refusal that follows a malformed one.
const keyScopeAttestationMaxResponseBytes = 4096

// keyScopeAttestationPrincipalParam is the query parameter a revocation names the principal in.
//
// A query parameter rather than a path segment, because the endpoint is a single
// operator-configured URL that may already carry a path, and appending a segment to an
// arbitrary URL is where escaping mistakes live. The value is escaped by url.Values.
const keyScopeAttestationPrincipalParam = "principal"

// ErrKeyScopeGatewayNotConfigured is returned by the constructor when the deployment has not
// declared a usable attestation endpoint.
//
// It is a sentinel rather than a formatted error because callers BRANCH on it: issuance treats
// "no gateway configured" as a state refusal it words for the operator, while a transport
// failure against a configured gateway is a different message entirely.
var ErrKeyScopeGatewayNotConfigured = errors.New(
	"event subscriber: no key-scope enforcement gateway is configured",
)

// KeyScopeBinding is what Blnk asks the gateway to hold and confirm for one principal.
//
// Every field is derived from the subscriber's registry row rather than from a request body, so
// a caller cannot widen what is attested. The prefix is the value stored in
// partition_key_prefix, passed through untouched — not trimmed, not normalised — because the
// attestation compares it byte-for-byte and a normalisation on this side would make Blnk and
// the component agree about a value neither of them stored.
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
//
// Three fields, and all three are checked. A component that returns 200 with an empty body
// attests nothing: KeyScopeEnforced would be false, and the refusal says so.
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
//
// It is an interface so that issuance can be tested against a conformance double, and so that
// a deployment that declares no gateway gets a nil client and the caller's own state refusal
// rather than a client that pretends to attest.
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
//
// It holds its own http.Client rather than sharing Blnk's: this client talks to exactly one
// operator-declared endpoint, it must not follow redirects, and its timeout is derived from the
// credential-issuance budget rather than from the 30-second default the webhook client uses.
type keyScopeGateway struct {
	endpoint string
	token    string
	timeout  time.Duration
	client   *http.Client
}

// NewKeyScopeGatewayClient builds the client from live configuration, or reports that none is
// declared.
//
// The decision of whether a gateway is usable belongs to config.KafkaConfig.KeyScopeAttestation
// and is not re-derived here, so the client this returns and the enforcement fact
// KeyScopeGateway reports can never disagree — a client that existed while enforcement was
// reported inactive would attest bindings for credentials that were being refused anyway, and
// the reverse would issue credentials with nothing verified.
//
// # Why the HTTP client is built here and not shared
//
//   - REDIRECTS ARE REFUSED. A 3xx from the control endpoint would otherwise send an
//     authenticated request, bearer token included, to whatever host the redirect named. That is
//     a credential-forwarding primitive, and there is no legitimate reason for a control
//     endpoint to redirect.
//   - The TIMEOUT is the configured attestation timeout, which is capped by the issuance
//     budget. A client-level timeout is belt-and-braces beside the per-call context deadline:
//     the context bounds the call, and this bounds a client that ignored it.
//   - Connection reuse is deliberate and small. Issuance is not a hot path, and a large pool
//     against one endpoint buys nothing.
//
// Parameters:
//   - cnf *config.Configuration: the live configuration. A nil configuration declares nothing.
//
// Returns:
//   - KeyScopeGatewayClient: the client, nil when no gateway is declared.
//   - error: ErrKeyScopeGatewayNotConfigured when nothing usable is declared. It is the only
//     error this returns; a declared endpoint is not dialled here.
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
// # The order matters: register, then believe
//
// The POST both registers and attests, in one call, because the two cannot be usefully
// separated. A component asked to attest a prefix it had never been told about could only
// guess, and a component told about a prefix in one call and asked about it in another leaves a
// window in which Blnk holds a confirmation for a binding that has since been dropped.
//
// # What is checked, and why each check exists
//
//   - 2xx. A 401 or 403 means Blnk's own token is wrong, which is a configuration fault worth
//     distinguishing in the log; a 404 usually means the URL names something that is not a
//     control endpoint. Both are refusals here.
//   - key_scope_enforced == true. A component may legitimately answer that it will not enforce
//     a scope — a topic it does not front, a prefix shape it cannot apply — and that is a
//     refusal rather than an error.
//   - The PRINCIPAL is echoed. A permissive proxy that answered 200 to everything without
//     reading the body would pass the first two checks and fail this one.
//   - The PREFIX is echoed BYTE-FOR-BYTE. This is the whole point. A component that trimmed,
//     lower-cased or truncated the prefix would be enforcing a DIFFERENT boundary from the one
//     the registry records and the credential response describes, and a wider one at that.
//
// # What is never done
//
// The bearer token is never logged and never included in an error. The component's `detail` is
// bounded and sanitised before it reaches a log field and never reaches an API response, on the
// same reasoning that keeps broker error text out of responses.
//
// Parameters:
//   - ctx context.Context: the caller's context. Bounded further by the attestation timeout.
//   - binding KeyScopeBinding: the binding, derived entirely from the registry row.
//
// Returns:
//   - error: nil when the component attested the exact binding; otherwise a typed
//     ErrSubscriberKeyScopeUnattested carrying a bounded, non-disclosing detail, with the
//     retryable flag set only for a transport-level failure.
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
// # Why this is called AFTER the broker credential is gone, and why it is not fatal
//
// The authoritative revocation is the broker's: deleting the SCRAM credential ends the
// principal's ability to authenticate anywhere, including at the gateway, since the gateway
// terminates the same SASL exchange. This call is the gateway's chance to drop state for a
// principal that no longer exists, which keeps its binding table from accumulating dead
// entries — a hygiene and audit concern rather than an access one.
//
// It is therefore best-effort by CONTRACT: the caller logs a failure and proceeds, because
// failing a deregistration whose broker half already succeeded would leave the operator with a
// subscriber that cannot be removed while a component is down, and the access it is trying to
// remove is already gone.
//
// A 404 is SUCCESS. A component that never held a binding for this principal — because
// issuance never got that far, or because it has already been revoked — is in exactly the state
// this call is asking for, and treating it as a failure would make a retried deregistration
// noisier than the first attempt.
//
// Parameters:
//   - ctx context.Context: the caller's context, typically a bounded cleanup context.
//   - principal string: the subscriber's Kafka principal.
//
// Returns:
//   - error: non-nil when the component answered neither 2xx nor 404, or could not be reached.
//     The caller decides what to do with it; nothing here is retried.
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

// Health reports whether the declared component answers and asserts key-scope enforcement.
//
// It names no principal, so it establishes only that something is there and claims to do the
// job — which is exactly what a start-up probe and a runbook check need, and deliberately less
// than AttestBinding establishes. A deployment whose gateway is unhealthy still refuses
// key-scoped issuance, because that refusal comes from the attestation call rather than from
// this one; this exists so an operator learns about it before a subscriber does.
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
//
// The per-call deadline is the configured attestation timeout applied to the CALLER's context,
// so whichever of the two is sooner governs: a caller with a nearly spent issuance budget is
// not given a fresh two seconds, and a caller with no deadline is still bounded.
//
// Parameters:
//   - ctx context.Context: the caller's context.
//   - method string: the HTTP method.
//   - target string: the absolute URL, which for a revocation carries the principal query.
//   - body []byte: the request body, nil for GET and DELETE.
//
// Returns:
//   - *http.Response: the response, whose body the caller must read through readBoundedBody.
//   - error: a transport-level failure only. A non-2xx status is a successful call.
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
//
// One constructor, so every failure path answers with the same code and the same shape. The
// CAUSE is never returned to the caller: it can carry the component's own error text, a DNS
// name or an internal address, and this endpoint is reachable by an operator holding the master
// key rather than by the subscriber. It is logged instead, bounded and classified.
//
// Parameters:
//   - binding KeyScopeBinding: names the subscriber in the log and the detail.
//   - message string: the operator-facing summary.
//   - retryable bool: true only for a transport-level failure, where repeating may succeed.
//   - cause error: logged, never returned.
//
// Returns:
//   - error: a typed apierror carrying ErrSubscriberKeyScopeUnattested.
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
//
// It is a constant rather than a formatted string because the values it would interpolate — the
// prefix Blnk sent and the prefix the component returned — are the boundary itself, and a
// response body is not where a boundary belongs. The log line carries the classification.
const attestationMismatchMessage = "the key-scope enforcement gateway attested a different " +
	"principal or a different partition key prefix from the one recorded on this subscriber"

// validate refuses a binding that could not honestly be attested.
//
// It guards the CALLER's construction rather than a request body — every field is derived from
// a registry row — so a failure here is a defect in Blnk, not a caller error. It exists because
// an empty principal or an empty prefix would produce an attestation that appeared to succeed
// while describing nothing: a component echoing two empty strings would satisfy the comparison.
//
// Returns:
//   - error: non-nil when the binding is not attestable.
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
//
// All three comparisons are exact. The prefix in particular is compared with ==, not with a
// case-insensitive or trimmed comparison: a component that returns a normalised form is
// enforcing a different boundary from the one recorded, and Kafka message keys are bytes.
//
// Parameters:
//   - binding KeyScopeBinding: what was sent.
//
// Returns:
//   - error: nil on an exact match; otherwise an error naming which of the three failed,
//     without quoting the prefix values.
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

// readBoundedBody reads and closes a response body, up to keyScopeAttestationMaxResponseBytes.
//
// Closing is unconditional so a connection is always released, and the cap is applied with
// io.LimitReader so an endless body cannot exhaust memory inside credential issuance. A body
// that exceeds the cap is TRUNCATED rather than reported: the truncated bytes then fail to
// decode, which is the same refusal a malformed body gets, and reporting the size separately
// would add a branch nobody acts on differently.
//
// Parameters:
//   - response *http.Response: may be nil, which reads as an empty body.
//
// Returns:
//   - []byte: the bytes read, possibly empty.
//   - error: a read failure. Not returned for an over-long body.
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
//
// It parses and re-encodes rather than concatenating, so an endpoint that already carries a
// query keeps it and the principal is escaped by url.Values. A principal containing an
// ampersand cannot therefore inject a second parameter — Blnk's principals are constrained to a
// safe alphabet, but a URL builder that relies on its input being safe is one that will be
// wrong the day the alphabet changes.
//
// Parameters:
//   - endpoint string: the configured control endpoint.
//   - principal string: the principal to revoke.
//
// Returns:
//   - string: the absolute URL to call.
//   - error: when the configured endpoint does not parse. config.KeyScopeAttestation has
//     already established that it does, so this cannot fire in practice.
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
//
// Scheme and host only. A redirect target's path or query can carry whatever the redirecting
// party chose, and this string reaches a log; the host is what an operator needs in order to
// understand which endpoint tried to hand the request somewhere else.
//
// Parameters:
//   - request *http.Request: the request the client was about to follow. May be nil.
//
// Returns:
//   - string: "scheme://host", or "unknown" when there is nothing to render.
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
//
// It exists so the ONE place that decides what is attested is beside the client that sends it:
// every field is read from the row, the prefix is taken through SubscriberPartitionKeyPrefix so
// the same accessor the rest of the file uses is the one that reads it, and no caller can pass
// a value of its own.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the row about to be provisioned. May be nil.
//
// Returns:
//   - KeyScopeBinding: the binding. Zero-valued for a nil subscriber, which validate refuses.
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
