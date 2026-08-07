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
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"

	"github.com/hibiken/asynq"
)

// This file is the LEGACY HTTP webhook transport. It is retained deliberately, and
// only for the duration of the 30-day dual-delivery window that accompanies the move
// to Kafka event streaming. Nothing in it is new work; everything in it is frozen.
//
// What changed when Kafka publishing landed is not the code here — it is who calls
// it. Before: eight domain post-actions called SendWebhook directly (the ledger,
// identity, balance, balance-monitor, transaction-execution, bulk-transaction and
// transaction-rejection sites, plus the notification.RegisterWebhookSender closure in
// blnk.go that carries system.error). After: none of them do. They record an event
// into blnk.event_outbox — inside the mutation's own transaction when the caller has
// one to share, and otherwise as its own committed insert from the post-action
// goroutine, which is what those call sites do today — and the event relay in
// event_relay.go claims that row, publishes it to Kafka, and, for as long as the
// window is open, enqueues the legacy webhook task from THAT SAME CLAIMED ROW through
// EnqueueLegacyWebhookDelivery. The relay's dual-delivery branch is therefore the one
// and only caller of that function.
//
// READING ONE ROW IS NECESSARY BUT NOT SUFFICIENT. Both transports reading one row is
// what makes them carry the same EVENT; carrying the same BYTES additionally requires
// that neither re-serialises the payload on its way out, and the byte-level equivalence
// assertion is what the guarantee is actually measured by. An earlier version of this
// contract claimed equivalence while the legacy path decoded the stored bytes into
// NewWebhook.Payload interface{} and marshalled them again — which sorts every object's
// keys and re-renders every number through float64. The two bodies stayed semantically
// equal, so nothing looked wrong; they were simply not the same bytes.
//
// The path is therefore byte-oriented end to end: EnqueueLegacyWebhookDelivery carries
// the stored bytes into the queue, ProcessWebhook validates without transforming, and
// processHTTPRaw signs and POSTs exactly what it was handed. Nothing between the outbox
// row and the socket re-serialises anything, which is what makes the equivalence
// structural rather than a property somebody has to remember to preserve.
//
// Deleting this file before the window closes would remove the second transport that
// the guarantee is measured against.
//
// This file holds NO sunset logic, and that is intentional. The deprecation window is
// one resolution with two consumers, each keying on the boundary it owns: the relay's
// dual-delivery branch asks WebhookDualDeliveryActive, which is true only inside
// [start, sunset), and the HTTP 410 Gone guard asks WebhookSunsetPassed, because a
// route's availability is a function of the sunset alone. Both live in
// event_sunset.go, the only place in this codebase that compares a clock against the
// configured window. A date comparison added here would be a third, independently
// drifting copy of that decision. The functions below simply deliver whatever they are
// handed, whenever they are called; deciding whether they should be called at all
// belongs to the caller.
//
// Retiring this file is a separate, later step with a strict order of operations,
// spelled out in the SUNSET block at the foot of this file. Read it before deleting
// anything: two symbols here must outlive the file, and one shared asynq queue must
// survive it untouched.

// NewWebhook is the webhook notification envelope, and it is a FROZEN CONTRACT.
// It includes an event type and associated payload data.
//
// Marshaled, it is the exact HTTP body Blnk has always POSTed to a subscriber: a
// two-key object, {"event": <string>, "data": <object>}. That same marshaled object
// is now carried verbatim — both keys, unaltered — as the payload of the Kafka
// LedgerEvent envelope, which is what lets an existing subscriber's body parser keep
// working when only the transport has changed.
//
// Consequently the field names, the field types and above all the JSON tags must not
// change. Renaming a tag, dropping the outer envelope in favour of the inner data
// object, or adding a field would silently break byte-equivalence between the two
// transports and, with it, every subscriber parser written against the HTTP era.
// The struct outlives this file: at sunset it moves, unmodified, to event_outbox.go.
type NewWebhook struct {
	Event   string      `json:"event"` // The event type that triggered the webhook.
	Payload interface{} `json:"data"`  // The data associated with the event.
}

// getEventFromStatus maps a transaction status to a corresponding event string.
//
// This function is the transaction event-string vocabulary. Seven of the thirteen
// event names Blnk emits originate here, and they are the names that route a Kafka
// message to a topic just as they used to name a webhook.
//
// THE TABLE ITSELF NOW LIVES IN model.EventTypeForTransactionStatus, and this is a
// one-line delegation to it. The table had to move because the repository layer
// derives event rows inside the atomic writers and needs the same mapping, and
// `model` cannot import the root package — so leaving the table here would have meant
// two tables, in two packages, mapping one status to an event name. Two tables that
// agree today is precisely what drift looks like before it happens: one status
// resolving to two different event names depending on which layer was looking would
// split one aggregate's events across two topics with nothing failing to say so.
//
// The behaviour is unchanged, including the case-insensitive comparison. Only the
// location of the table changed, which is also the sunset relocation the AAP requires:
// when webhooks.go is deleted this function goes with it, and the vocabulary it used
// to own is already somewhere that survives.
//
// DELIBERATELY PRESERVED DEFECT — do not "fix" this in passing. StatusCommit
// ("COMMIT", declared in transaction_inflight.go) has no case in the table, so it
// falls through to transaction.unknown. That is pre-existing behaviour, not a
// regression introduced by the Kafka work, and it is kept exactly as-is on purpose:
// the dual-delivery comparison asserts that the Kafka message and the legacy webhook
// carry identical bytes for the same event, and adding a transaction.commit case
// would change one side of that comparison and fail it for a reason that has nothing
// to do with the transport. The behaviour is documented in docs/event-streaming.md so
// it can be corrected later as a deliberate, separately reviewed change — with the
// subscriber-facing event-name change that implies.
//
// Parameters:
// - status string: The status of the transaction.
//
// Returns:
// - string: The corresponding event string for the transaction status.
func getEventFromStatus(status string) string {
	return model.EventTypeForTransactionStatus(status)
}

// legacyWebhookPrivateDestinationWarning makes the operator's private-destination
// assertion appear in the log exactly once per process.
//
// Once, rather than per delivery: at any real delivery rate a per-dial warning becomes
// noise that gets filtered, and a filtered warning is not a warning. Once per process is
// enough for the assertion to be discoverable in the log of any deployment that made it,
// which is the whole purpose.
var legacyWebhookPrivateDestinationWarning sync.Once

// errLegacyWebhookRedirect is the sentinel every refused redirect wraps.
//
// A sentinel rather than a formatted string so a test can assert the REASON a delivery
// failed with errors.Is instead of matching prose, and so a future caller can distinguish
// "the endpoint tried to redirect us" from a transport failure it should retry
// differently.
var errLegacyWebhookRedirect = errors.New("legacy webhook delivery refused to follow a redirect")

// errLegacyWebhookDestination is the sentinel every refused destination wraps.
var errLegacyWebhookDestination = errors.New("legacy webhook destination is not permitted")

// legacyWebhookAllowsPrivateDestination reports whether the operator has asserted that
// the webhook destination is on a network they own.
//
// Configuration is read on every call rather than captured when the client was built.
// That is deliberate: the client is constructed once for the process lifetime, so a
// captured value would freeze whatever configuration happened to be loaded at
// construction and ignore every later reload.
//
// It FAILS CLOSED. Unfetchable configuration answers false, so an unconfigured process
// refuses internal destinations rather than permitting them by accident.
//
// Returns:
//   - bool: true only when configuration loads and the assertion is set.
func legacyWebhookAllowsPrivateDestination() bool {
	conf, err := config.Fetch()
	if err != nil || conf == nil {
		return false
	}

	return conf.Notification.Webhook.AllowPrivateDestination
}

// refuseLegacyWebhookRedirect is the http.Client CheckRedirect hook for the legacy
// transport. It never permits a redirect.
//
// Parameters:
//   - req *http.Request: the request the client is about to make to the redirect target.
//   - via []*http.Request: the requests already made, so the hop count can be reported.
//
// Returns:
//   - error: always non-nil, wrapping errLegacyWebhookRedirect.
func refuseLegacyWebhookRedirect(req *http.Request, via []*http.Request) error {
	target := "unknown"
	if req != nil && req.URL != nil {
		// Scheme and host only. The path can carry a subscriber's own identifiers, and
		// the whole value is third-party text, so it is bounded and stripped of control
		// characters before it reaches a log line.
		target = sanitizeLogValue(req.URL.Scheme+"://"+req.URL.Host, maxLoggedErrorLength)
	}

	logrus.WithFields(logrus.Fields{
		"redirect_target": target,
		"hops":            len(via),
	}).Warn("legacy webhook delivery refused to follow a redirect; a webhook POST is " +
		"never redirected legitimately and following one is how a subscriber endpoint " +
		"reaches Blnk's internal network")

	return fmt.Errorf("%w: %s after %d hop(s)", errLegacyWebhookRedirect, target, len(via))
}

// guardLegacyWebhookDial is the net.Dialer Control hook for the legacy transport. It runs
// after DNS resolution and before connect, and refuses an address Blnk must not reach.
//
// # Why here and not before the request
//
// This is the only point at which the address actually being connected to is known. A
// check on the URL text judges a name; this judges the answer. That closes DNS rebinding,
// and it closes it without a time-of-check-to-time-of-use window, because the address
// handed to this hook is the address the socket then uses — there is no second lookup to
// disagree with.
//
// Parameters:
//   - network string: the dial network, e.g. "tcp4". A non-TCP network is refused.
//   - address string: "host:port", where host is always a resolved literal.
//   - _ syscall.RawConn: the raw connection, unused. No socket option is set here.
//
// Returns:
//   - error: non-nil to abandon this address, wrapping errLegacyWebhookDestination.
func guardLegacyWebhookDial(network, address string, _ syscall.RawConn) error {
	if !strings.HasPrefix(network, "tcp") {
		return fmt.Errorf("%w: %s is not a TCP network", errLegacyWebhookDestination,
			sanitizeLogValue(network, maxLoggedErrorLength))
	}

	host, _, err := net.SplitHostPort(address)
	if err != nil {
		// Unparseable means the guard cannot judge it, and an unjudged address is
		// refused. Permitting what could not be understood is how deny-lists fail.
		return fmt.Errorf("%w: dial address could not be parsed", errLegacyWebhookDestination)
	}

	parsed := net.ParseIP(host)
	reason := model.InternalIPReason(parsed)
	if reason == "" {
		return nil
	}

	if legacyWebhookAllowsPrivateDestination() && model.OperatorOwnableInternalIP(parsed) {
		legacyWebhookPrivateDestinationWarning.Do(func() {
			logrus.WithField("reason", reason).Warn(
				"legacy webhook delivery reached an internal address because " +
					"notification.webhook.allow_private_destination is set; the operator has " +
					"asserted this network is theirs. Link-local, metadata, multicast, " +
					"unspecified and NAT64-wrapped addresses remain refused")
		})

		return nil
	}

	logrus.WithFields(logrus.Fields{
		"resolved_address": hashLogIdentifier(host),
		"reason":           reason,
	}).Error("legacy webhook delivery refused an internal destination; the configured " +
		"endpoint resolved, or redirected, to an address inside Blnk's own network")

	return fmt.Errorf("%w: the destination resolved to an address Blnk must not reach — %s",
		errLegacyWebhookDestination, reason)
}

// validateLegacyWebhookDestination applies the URL-text half of the destination policy to
// the configured endpoint.
//
// It is the cheap, early half. It rejects the schemes and the obviously-internal hosts
// before a request is built, so the common misconfiguration produces one clear error
// instead of a dial failure an operator has to interpret. The authoritative half is
// guardLegacyWebhookDial, which judges what the name actually resolves to — this function
// deliberately claims nothing about that.
//
// Names are treated as operator-ownable when the assertion is set, IP literals are not.
// The asymmetry is the point: a name cannot be identified as the metadata endpoint by
// inspection (metadata.google.internal is just a .internal name), so nothing is gained by
// tiering names here — the dial hook judges the address it resolves to, and that hook
// refuses link-local no matter what the assertion says.
//
// Parameters:
//   - rawURL string: the configured destination. Never empty; callers check first.
//
// Returns:
//   - error: non-nil when the destination is refused, wrapping
//     errLegacyWebhookDestination, and naming only the host.
func validateLegacyWebhookDestination(rawURL string) error {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		// The parse error is not echoed: it quotes the input, and the input is a third
		// party's endpoint that has no business in Blnk's logs verbatim.
		return fmt.Errorf("%w: the configured URL is not parseable", errLegacyWebhookDestination)
	}

	allowPrivate := legacyWebhookAllowsPrivateDestination()

	switch parsed.Scheme {
	case "https":
	case "http":
		if !allowPrivate {
			return fmt.Errorf("%w: https is required so that ledger data and its signature "+
				"are not sent in clear text; set notification.webhook.allow_private_destination "+
				"only for a destination on a network you own", errLegacyWebhookDestination)
		}
	default:
		// file://, gopher://, ftp:// and friends turn a URL field into a local-resource
		// read. No assertion opens them.
		return fmt.Errorf("%w: scheme %q is not an HTTP scheme", errLegacyWebhookDestination,
			sanitizeLogValue(parsed.Scheme, maxLoggedErrorLength))
	}

	host := parsed.Hostname()
	if host == "" {
		return fmt.Errorf("%w: the configured URL has no host", errLegacyWebhookDestination)
	}

	reason := model.InternalDestinationReason(host)
	if reason == "" {
		return nil
	}

	if allowPrivate {
		if literal := net.ParseIP(host); literal == nil || model.OperatorOwnableInternalIP(literal) {
			return nil
		}
	}

	return fmt.Errorf("%w: host %q is internal — %s", errLegacyWebhookDestination,
		sanitizeLogValue(host, maxLoggedErrorLength), reason)
}

// processHTTP sends a webhook notification via HTTP POST request.
//
// The wire contract it implements is unchanged by the move to Kafka and is asserted
// end to end by webhooks_process_test.go, which reconstructs the signature from what
// the receiver was handed:
//
//   - The body is the marshaled NewWebhook envelope, sent with
//     Content-Type: application/json.
//   - When server.secret_key is configured, X-Blnk-Timestamp carries the unix second
//     and X-Blnk-Signature carries the hex-encoded HMAC-SHA256 of
//     timestamp + "." + body under that key. The timestamp is inside the signed
//     material, which is what lets a receiver reject replays. When no secret is
//     configured the request goes out unsigned and a warning says so.
//   - Configured Notification.Webhook.Headers are applied, EXCEPT the ones the transport
//     computes for itself — see applyConfiguredWebhookHeaders, which explains why a
//     configured X-Blnk-Signature is an integrity problem rather than an override.
//
// Retry is delegated, not owned. A non-2xx response returns an error so that asynq
// retries the delivery; swallowing it would permanently drop the webhook on a
// receiver-side failure. Exactly one HTTP attempt happens per call. The Kafka relay
// deliberately does NOT reuse this arrangement — a Kafka publish has no HTTP status
// to key on, so event_relay.go owns its own bounded exponential backoff. That
// difference lives entirely in the relay and changes nothing here.
//
// Parameters:
// - ctx context.Context: bounds the request. Cancelling it abandons the delivery.
// - data NewWebhook: The webhook notification data to send.
// - client *http.Client: The HTTP client to use for the request.
//
// Returns:
// - error: An error if the request or processing fails.
func processHTTP(ctx context.Context, data NewWebhook, client *http.Client) error {
	payloadBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return processHTTPRaw(ctx, payloadBytes, client)
}

// processHTTPRaw sends an already-serialised webhook body, signing and posting the exact
// bytes it was handed.
//
// # Why this is the primitive and processHTTP is the wrapper
//
// This is the only function that touches the socket, and it takes BYTES. That is what makes
// byte fidelity structural rather than a property somebody has to remember to preserve:
// there is no path to the network that re-serialises anything, so the body a subscriber
// receives is necessarily the body the caller supplied. For the dual-delivery window that
// body is blnk.event_outbox.payload_raw, the byte-exact stored copy, identical to the bytes
// published to Kafka.
//
// processHTTP remains as a thin wrapper for the struct-shaped callers — the existing tests
// and any caller that genuinely holds a NewWebhook — and marshals once before delegating
// here. The important consequence is the direction of the dependency: serialisation happens
// ABOVE this function or not at all, never inside it.
//
// The signature covers the bytes as sent. Signing the supplied body rather than a
// re-serialisation of it is not a detail: a receiver recomputes HMAC over the body it
// received, so a body that changed between signing and sending would fail verification at
// every subscriber.
//
// Every other element of the wire contract is unchanged from processHTTP's documentation
// above — content type, the timestamp inside the signed material, the unsigned warning when
// no secret is configured, configured headers filtered by applyConfiguredWebhookHeaders, and
// a non-2xx returned so asynq retries.
//
// # THE REQUEST IS BOUND TO THE CALLER'S CONTEXT
//
// It is built with http.NewRequestWithContext, not http.NewRequest. asynq cancels a handler's
// context when a worker begins shutting down, and with an unbound request that cancellation
// reached nothing: the only bound on a delivery was the shared client's thirty-second
// timeout, so a worker draining for deployment had to wait out every delivery already on the
// wire, and a receiver holding connections open could stretch that to thirty seconds per
// in-flight task. A cancelled delivery costs one redelivery — the task fails, asynq retries
// it, and the outbox row behind it is untouched — which is a far cheaper thing to pay than a
// deployment that cannot drain.
//
// # THIS FUNCTION LOGS NOTHING ABOUT A FAILED DELIVERY
//
// It returns the failure, status code included, and its callers record it once. It used to
// warn about a non-2xx here as well, which produced two records for one failure — a status
// with no event, task or queue beside it, and then the caller's record saying the same thing
// in different words. At the pipeline's 500-events-per-second target that duplicate is not a
// second opinion, it is half the log. See legacyWebhookFailureRecord for what replaced it.
//
// Parameters:
//   - ctx context.Context: bounds the request. Cancelling it abandons the delivery.
//   - payloadBytes []byte: the body to send, verbatim. Never inspected, never rewritten.
//   - client *http.Client: the pooled client to send with.
//
// Returns:
//   - error: a configuration, request-construction, transport or non-2xx failure.
func processHTTPRaw(ctx context.Context, payloadBytes []byte, client *http.Client) error {
	conf, err := config.Fetch()
	if err != nil {
		return err
	}

	// THE DESTINATION IS RE-JUDGED HERE, on every delivery, rather than once when the URL
	// was configured. Configuration is reloadable and this function is the last code that
	// runs before a request exists, so a URL that became unacceptable after the task was
	// enqueued is refused here rather than delivered. It is the cheap half of the policy;
	// guardLegacyWebhookDial judges what the host actually resolves to.
	if err := validateLegacyWebhookDestination(conf.Notification.Webhook.Url); err != nil {
		return err
	}

	secret := conf.Server.SecretKey

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		conf.Notification.Webhook.Url,
		bytes.NewBuffer(payloadBytes),
	)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")

	if secret != "" {
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		signatureData := timestamp + "." + string(payloadBytes)
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write([]byte(signatureData))
		signature := hex.EncodeToString(mac.Sum(nil))
		req.Header.Set("X-Blnk-Signature", signature)
		req.Header.Set("X-Blnk-Timestamp", timestamp)
	} else {
		warnWebhookSentUnsigned()
	}

	applyConfiguredWebhookHeaders(req.Header, conf.Notification.Webhook.Headers)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	// DRAIN BEFORE CLOSE, or the pooled connection is thrown away on every delivery.
	//
	// Go returns a connection to the transport's idle pool only when the response body has
	// been read to EOF and then closed. A Close on an unread body closes the CONNECTION
	// instead — so with no drain here, initializeHTTPClient's MaxIdleConns of 100 and
	// MaxIdleConnsPerHost of 10 pool nothing at all, and every webhook delivery pays for a
	// fresh TCP handshake (and a fresh TLS handshake, on an https receiver) even though the
	// client is shared. Measured on the receiver: five sequential deliveries opened five
	// connections without this drain and one with it.
	//
	// The response body is otherwise of no interest — the status decides success, and
	// nothing here parses what a subscriber sends back — which is exactly why the read was
	// easy to omit and impossible to notice: delivery works perfectly either way.
	//
	// BOUNDED, because the receiver is not ours. An unbounded io.Copy would let a broken or
	// hostile endpoint stream unlimited data into a webhook worker for as long as the
	// client timeout allows, turning a delivery into a memory-and-bandwidth sink. A body
	// larger than the cap simply does not get its connection reused, which is the correct
	// trade: reuse is an optimisation and the bound is a safety property.
	//
	// The read error is deliberately ignored. A drain that fails leaves the connection
	// unreusable and nothing else — the delivery has already happened, and the status below
	// is what determines whether asynq retries it. Reporting a drain failure as a delivery
	// failure would cause a retry of a webhook the subscriber already received.
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxWebhookResponseDrainBytes))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Returning the error lets asynq retry the delivery; swallowing it would
		// permanently drop the webhook on receiver-side failures. The status is carried IN
		// the error rather than logged here, so the caller's single record can name it
		// alongside the event, task and queue it belongs to.
		return fmt.Errorf("webhook delivery failed with status %d", resp.StatusCode)
	}

	return nil
}

// transportOwnedWebhookHeaders are the headers processHTTPRaw computes for itself, in the
// canonical form net/http stores them under.
//
// They are listed rather than derived because each one carries a guarantee the delivery makes
// about ITSELF, which no amount of configuration can be allowed to restate:
//
//   - X-Blnk-Signature and X-Blnk-Timestamp are the HMAC over timestamp + "." + body and the
//     timestamp it is computed against. They are the only evidence a subscriber has that the
//     body came from this deployment.
//   - Content-Type describes a body this function marshalled. The body is always JSON, and a
//     receiver told otherwise mis-parses a payload that is perfectly well formed.
//   - Content-Length is set by net/http from the body it is actually sending; a configured
//     value would either truncate the request or make it hang waiting for bytes.
//
// The map is keyed by http.CanonicalMIMEHeaderKey's output because Header.Set canonicalises
// what it is given: a configured "x-blnk-timestamp" and "X-Blnk-Timestamp" land on the same
// entry, so comparing raw configured keys would let the lower-cased spelling through.
var transportOwnedWebhookHeaders = map[string]struct{}{
	textproto.CanonicalMIMEHeaderKey("Content-Type"):     {},
	textproto.CanonicalMIMEHeaderKey("Content-Length"):   {},
	textproto.CanonicalMIMEHeaderKey("X-Blnk-Signature"): {},
	textproto.CanonicalMIMEHeaderKey("X-Blnk-Timestamp"): {},
}

// applyConfiguredWebhookHeaders copies the operator's configured headers onto an outgoing
// request, refusing the ones the transport owns.
//
// # The defect this closes, which is an integrity defect rather than a tidiness one
//
// Configured headers used to be applied last, so a header named X-Blnk-Signature simply
// REPLACED the computed HMAC with whatever constant was configured. Every subscriber that
// actually verifies the signature would then reject every delivery — noisy, and therefore
// survivable. The dangerous half is the subscriber that only checks the header is PRESENT: it
// accepts the constant for ever, including for a body it should have refused, and the
// deployment believes it is signing its webhooks the whole time. Neither side logs anything.
//
// # Why the refusal is narrow
//
// Only the four headers the transport computes are refused. A configured Authorization,
// X-Tenant or User-Agent is exactly what this setting is for — the receiver's own
// requirements — and filtering by prefix or by an allowlist would break that. The rule is
// "the transport's own statements about this request are not configurable", nothing wider.
//
// # Why the refusal is a warning rather than an error
//
// The delivery still happens, correctly signed. Refusing the whole delivery would turn a
// configuration mistake into an outage for every subscriber on a transport that is already
// deprecated, and the mistake is visible in the log either way. The warning is rate-limited
// on the same reasoning as the unsigned-delivery one: the condition is static configuration
// re-read on every delivery, so warning per delivery produces hundreds of identical lines a
// second and buries everything else.
//
// Parameters:
//   - header http.Header: the outgoing request's headers, already carrying the computed ones.
//   - configured map[string]string: the operator's configured headers. A nil map is a no-op.
func applyConfiguredWebhookHeaders(header http.Header, configured map[string]string) {
	refused := make([]string, 0, len(configured))

	for key, value := range configured {
		if _, owned := transportOwnedWebhookHeaders[textproto.CanonicalMIMEHeaderKey(key)]; owned {
			refused = append(refused, textproto.CanonicalMIMEHeaderKey(key))

			continue
		}

		header.Set(key, value)
	}

	if len(refused) == 0 {
		return
	}

	// Sorted, so the same misconfiguration reads the same way every time; a Go map's range
	// order would make two lines about one problem look like two problems.
	sort.Strings(refused)

	warnConfiguredWebhookHeadersRefused(refused)
}

// configuredWebhookHeaderWarning rate-limits the shadowed-header warning for the process, for
// the same reason unsignedWebhookWarning is package-level: the condition describes the
// deployment's configuration rather than any one delivery, so one budget must cover every
// delivery site or the limit means nothing.
var configuredWebhookHeaderWarning = &rateLimitedWarning{
	interval: unsignedWebhookWarningInterval,
	now:      time.Now,
}

// warnConfiguredWebhookHeadersRefused reports refused headers at most once per
// unsignedWebhookWarningInterval, naming which ones and how many deliveries the silence
// covered.
//
// The names are safe to log: they are header FIELD NAMES from the deployment's own
// configuration and are matched against a fixed set before they get here, so the value —
// which may be a credential — is never named.
//
// Parameters:
//   - refused []string: the canonical names that were not applied, already sorted.
func warnConfiguredWebhookHeadersRefused(refused []string) {
	emit, suppressed := configuredWebhookHeaderWarning.admit()
	if !emit {
		return
	}

	entry := logrus.WithFields(logrus.Fields{
		"refused_headers":  strings.Join(refused, ","),
		"warning_interval": unsignedWebhookWarningInterval.String(),
	})
	if suppressed > 0 {
		entry = entry.WithField("deliveries_suppressed_since_last_warning", suppressed)
	}

	entry.Warn(
		"webhook headers ignored: notification.webhook.headers may not set the headers the " +
			"transport computes for itself, because a configured value there would replace the " +
			"signature, its timestamp or the content type of a body Blnk marshalled",
	)
}

// maxWebhookResponseDrainBytes bounds how much of a webhook receiver's response body is read
// before the connection is returned to the idle pool.
//
// 64 KiB is far more than any acknowledgement a receiver has cause to send — Blnk reads none
// of it, and the status line is what decides success — while being small enough that a
// receiver which streams a response cannot use a delivery as a memory sink. Above the cap the
// connection is not reused; nothing else changes.
const maxWebhookResponseDrainBytes = 64 << 10

// unsignedWebhookWarningInterval is the shortest gap between two warnings about the same
// unconfigured signing key.
//
// Ten minutes is chosen against the failure it prevents rather than as a round number. The
// condition is STATIC — server.secret_key is either configured or it is not — so its
// information content is one bit, and the pipeline's acceptance target is 500 events per
// second. Warning per delivery therefore produced up to 500 identical lines a second, which
// is not a louder warning but a quieter one: it buries every other line in the log, and an
// operator who has seen the first thousand stops reading. Ten minutes keeps the condition
// continuously visible to anyone reading a window of log, at a cost of six lines an hour.
// legacyWebhookTaskIDNamespace prefixes an event id to form the task identity.
//
// It is declared once and shared by legacyWebhookTaskID and legacyWebhookEventID because
// those two are inverses. Spelling it twice would let the decoder drift from the encoder,
// and the symptom would be silent: task ids would still be namespaced correctly, and the
// failure record would simply stop reporting event_id.
const legacyWebhookTaskIDNamespace = "legacy-webhook:"

const unsignedWebhookWarningInterval = 10 * time.Minute

// rateLimitedWarning admits at most one occurrence of a repeating condition per interval
// and counts the ones it withholds.
//
// # Why a latch would be wrong
//
// The obvious implementation is sync.Once: warn the first time and never again. It is
// wrong here because the condition is not permanent — configuration is re-read on every
// delivery (see ProcessWebhook), so a secret key can be removed from a running
// deployment. A latch would have spent its single warning during a correctly configured
// period and then stayed silent through the exposure it exists to report. An interval
// re-warns for as long as the condition lasts and stops when it stops.
//
// # Why the count is unsigned and never resets to a lie
//
// suppressed counts occurrences since the last EMITTED warning, and admit hands that count
// to the caller and clears it in the same critical section. Two callers can therefore never
// both report the same suppressed occurrences, and none is dropped: whichever wins the lock
// reports them.
//
// The zero value is usable and warns on its first occurrence: emitted is false until the
// first admission, which is what distinguishes "never warned" from "warned at the zero
// time" — an interval comparison against a zero time.Time would technically also admit,
// but only by accident of the epoch being far in the past.
type rateLimitedWarning struct {
	mu sync.Mutex

	// interval is the minimum gap between emitted warnings. A non-positive interval
	// emits every occurrence, which is the pre-rate-limit behaviour and is useful in a
	// test that wants to observe every one.
	interval time.Duration

	// now is the clock, injectable so a test can advance time without sleeping. Never
	// nil in the package-level instances; admit falls back to time.Now if it is, so a
	// zero-value struct is still usable.
	now func() time.Time

	emitted     bool
	lastEmitted time.Time
	suppressed  uint64
}

// admit records one occurrence of the condition and decides whether it should be logged.
//
// Returns:
//   - bool: true when the caller should log this occurrence.
//   - uint64: when logging, how many occurrences were withheld since the previous logged
//     one, not counting this one. Zero when nothing was withheld. Meaningless when the
//     first return value is false.
func (w *rateLimitedWarning) admit() (bool, uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	clock := w.now
	if clock == nil {
		clock = time.Now
	}

	at := clock()

	if w.emitted && w.interval > 0 && at.Sub(w.lastEmitted) < w.interval {
		w.suppressed++
		return false, 0
	}

	withheld := w.suppressed
	w.suppressed = 0
	w.emitted = true
	w.lastEmitted = at

	return true, withheld
}

// unsignedWebhookWarning rate-limits the unsigned-delivery warning for the process.
//
// It is package-level state because the condition is package-level: it describes the
// deployment's configuration, not any one delivery, so every delivery site must share one
// budget or the limit means nothing. It is created here rather than lazily so there is no
// initialisation race between concurrent workers.
var unsignedWebhookWarning = &rateLimitedWarning{
	interval: unsignedWebhookWarningInterval,
	now:      time.Now,
}

// warnWebhookSentUnsigned reports an unsigned delivery at most once per
// unsignedWebhookWarningInterval, saying how many deliveries the suppressed interval
// covered.
//
// The count is the part that makes rate limiting honest. A bare "warned once every ten
// minutes" hides the scale of the exposure — one unsigned test delivery and forty thousand
// unsigned production deliveries produce the same line — so the number of occurrences the
// silence covered is reported with the warning that ends it.
func warnWebhookSentUnsigned() {
	emit, suppressed := unsignedWebhookWarning.admit()
	if !emit {
		return
	}

	entry := logrus.WithField("warning_interval", unsignedWebhookWarningInterval.String())
	if suppressed > 0 {
		entry = entry.WithField("deliveries_suppressed_since_last_warning", suppressed)
	}

	entry.Warn("webhook sent unsigned: server.secret_key is not configured")
}

// legacyWebhookFailureRecord assembles the one structured record a failed legacy delivery
// produces, naming what failed as precisely as the available identity allows.
//
// # What it names, and why each field earns its place
//
//   - task_id and queue come from the asynq handler context and are what an operator needs
//     to find, inspect or archive the task with asynqmon or the Inspector.
//   - event_id is decoded from the task id when the relay's namespace is present. It is the
//     join to blnk.event_outbox and to the Kafka message published from the same row, which
//     is what makes a webhook failure diagnosable against the event rather than in
//     isolation. Absent for a task that did not come from the relay — SendWebhook sets no
//     task identity at all — and absent rather than guessed.
//   - retry_count and max_retry say whether asynq will try again or is about to archive the
//     task. Without them a failure line cannot be told from a final one.
//   - event_type is the envelope's event name, which is what attributes the failure to a
//     subscriber-visible event family. Empty on the malformed-payload path, where by
//     definition nothing could be decoded.
//   - payload_bytes is the body size. It is the only fact available about a body that would
//     not parse, and it distinguishes an empty task from a truncated one.
//
// The context getters return ok=false for a context asynq did not create, and every such
// field is then OMITTED. A record that says retry_count=0 for a context that never carried
// one is worse than a record that says nothing: it reads as a first attempt.
//
// # Bounding
//
// event_type and task_type reach this function from stored data and from configuration, and
// the error text from a dependency; all three are passed through sanitizeLogValue, which
// strips the characters that let a value forge a log line and caps the length. The caps
// differ by kind deliberately: an identifier that is long is malformed, an error that is
// long may still be useful for its first few hundred characters.
//
// Parameters:
//   - ctx context.Context: the asynq handler context, read for identity only.
//   - task *asynq.Task: the task being processed. Read for its type and payload length.
//   - envelope NewWebhook: the decoded envelope, or the zero value when it could not be
//     decoded.
//   - cause error: the failure. Never nil at any call site; a nil cause simply contributes
//     no error field.
//
// Returns:
//   - *logrus.Entry: the assembled record, ready for the caller to give a message. Returned
//     rather than logged so the caller owns the message and the level stays visible at the
//     call site.
func legacyWebhookFailureRecord(ctx context.Context, task *asynq.Task, envelope NewWebhook, cause error) *logrus.Entry {
	fields := logrus.Fields{
		"transport": "legacy_webhook",
	}

	if task != nil {
		fields["task_type"] = sanitizeLogValue(task.Type(), maxLoggedFilterLength)
		fields["payload_bytes"] = len(task.Payload())
	}

	if taskID, ok := asynq.GetTaskID(ctx); ok {
		fields["task_id"] = sanitizeLogValue(taskID, maxLoggedFilterLength)

		if eventID := legacyWebhookEventID(taskID); eventID != "" {
			fields["event_id"] = sanitizeLogValue(eventID, maxLoggedFilterLength)
		}
	}

	if queue, ok := asynq.GetQueueName(ctx); ok {
		fields["queue"] = sanitizeLogValue(queue, maxLoggedFilterLength)
	}

	if retries, ok := asynq.GetRetryCount(ctx); ok {
		fields["retry_count"] = retries
	}

	if maxRetry, ok := asynq.GetMaxRetry(ctx); ok {
		fields["max_retry"] = maxRetry
	}

	if envelope.Event != "" {
		fields["event_type"] = sanitizeLogValue(envelope.Event, maxLoggedFilterLength)
	}

	if cause != nil {
		fields["error"] = sanitizeLogValue(cause.Error(), maxLoggedErrorLength)
	}

	return logrus.WithFields(fields)
}

// legacyWebhookEventID recovers the outbox event id from a task id that
// legacyWebhookTaskID produced.
//
// It is the exact inverse of that function and must stay so: the two are the only reason a
// webhook failure can be joined to the outbox row and the Kafka message that share its
// event. A task id from any other producer on the shared queue lacks the namespace and
// yields the empty string, which is how the caller knows to omit the field rather than
// report a foreign id as an event.
//
// Parameters:
//   - taskID string: the asynq task id, as the handler context reported it.
//
// Returns:
//   - string: the event id, or "" when the task did not come from the relay.
func legacyWebhookEventID(taskID string) string {
	eventID, found := strings.CutPrefix(taskID, legacyWebhookTaskIDNamespace)
	if !found {
		return ""
	}

	return strings.TrimSpace(eventID)
}

// LegacyWebhookRetention is how long a completed legacy-delivery task is kept in Redis.
//
// It exists solely to give the event-ID task identity a window in which it can actually
// suppress a duplicate. asynq deletes a task the moment it completes unless a retention is
// set, and a deleted task's ID is immediately reusable — so without this, a re-enqueue of
// the same event after the first delivery finished would be accepted, which is precisely
// the duplicate the identity is there to stop. Twenty-four hours comfortably covers a relay
// crash, an operator-initiated replay of a claimed batch, and a Redis failover, while
// bounding what the queue retains.
const LegacyWebhookRetention = 24 * time.Hour

// EnqueueLegacyWebhookDelivery enqueues the legacy HTTP delivery of ONE outbox event,
// carrying the stored bytes verbatim and using the event ID as the task's identity.
//
// # Why a byte-oriented entry point exists at all
//
// SendWebhook takes a NewWebhook STRUCT, and that is the problem it exists to solve. The
// relay holds the authoritative bytes of the legacy body: the bytes stored in
// blnk.event_outbox.payload_raw (BYTEA), which is the column that preserves them exactly,
// and the same bytes published to Kafka. The payload JSONB column beside it is a queryable
// projection of the same object — parsed and re-rendered, so key order and number
// formatting are not preserved — and is never the source of a body. Handing the bytes to
// SendWebhook would mean decoding them into
// NewWebhook.Payload interface{} and marshalling again, and a round trip through
// interface{} does not preserve bytes:
//
//   - Every JSON object becomes a map[string]interface{}, and Go marshals map keys in
//     SORTED order. A payload whose struct fields were serialised in declaration order
//     comes back alphabetised.
//   - Every JSON number becomes a float64. A large integer identifier re-renders in
//     scientific notation, and a decimal amount can gain or lose its trailing digits.
//
// The dual-delivery guarantee is that both transports carry the SAME payload for the same
// event, and it is asserted byte-for-byte. Re-marshalling breaks it while looking like it
// preserves it, because the two bodies remain semantically equal — which is exactly the
// kind of difference a reviewer's eye passes over and a byte comparison does not.
//
// So this path never decodes. It validates that the bytes are a well-formed legacy envelope,
// and then carries them unchanged all the way to the socket.
//
// # Why the event ID is the task ID
//
// Enqueuing the task and recording that the webhook leg was dispatched are two separate
// operations against two separate systems, and no transaction spans them. A crash between
// them leaves the row not-yet-marked, so the next claim of that row enqueues the delivery
// again — a duplicate webhook for one event.
//
// asynq's TaskID makes that harmless: a second enqueue under an ID already present is
// refused with ErrTaskIDConflict rather than accepted. That conflict is therefore SUCCESS
// for this operation's purposes — the task the caller wants enqueued is already enqueued —
// and it is reported as such rather than as an error, because treating it as a failure would
// stall the row it belongs to behind a condition that is already satisfied.
//
// Parameters:
//   - eventID string: the outbox row's event_id, which is the task's identity. Required:
//     without it there is no dedup key, and an empty asynq task ID is silently ignored
//     rather than rejected, so the caller would get at-least-once with no indication.
//   - body []byte: the stored legacy webhook body, carried verbatim.
//
// Returns:
//   - error: nil when the task is enqueued, and nil when an identical task was already
//     enqueued. Non-nil for a missing event ID, a body that is not a legacy envelope, or an
//     enqueue failure.
func (b *Blnk) EnqueueLegacyWebhookDelivery(eventID string, body []byte) error {
	conf := b.Config()

	// The same no-op-when-unconfigured contract SendWebhook has. See its documentation.
	if conf.Notification.Webhook.Url == "" {
		return nil
	}

	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return fmt.Errorf(
			"legacy webhook delivery requires the event id as its task identity; without one asynq cannot " +
				"suppress a duplicate enqueue after a crash between enqueuing and marking the row",
		)
	}

	// Validation, NOT transformation. The bytes are checked to be a legacy envelope and are
	// then forwarded untouched — the decoded value is deliberately discarded, because using
	// it is the defect this function exists to avoid.
	var envelope NewWebhook
	if err := json.Unmarshal(body, &envelope); err != nil {
		return fmt.Errorf("legacy webhook body for event %s is not a valid webhook envelope: %w", eventID, err)
	}

	task := asynq.NewTask(
		conf.Queue.WebhookQueue,
		body,
		asynq.Queue(conf.Queue.WebhookQueue),
		asynq.TaskID(legacyWebhookTaskID(eventID)),
		asynq.Retention(LegacyWebhookRetention),
	)

	if _, err := b.asynqClient.Enqueue(task); err != nil {
		if errors.Is(err, asynq.ErrTaskIDConflict) {
			// Already enqueued, in flight, or completed within the retention window. The
			// post-condition the caller needs — this event's webhook leg is queued exactly
			// once — holds, so this is success.
			logrus.WithFields(logrus.Fields{
				"event_id": eventID,
				"task_id":  legacyWebhookTaskID(eventID),
			}).Debug("legacy webhook delivery for this event is already enqueued; the duplicate was suppressed")

			return nil
		}

		logrus.WithError(err).WithField("event_id", eventID).Error("could not enqueue legacy webhook delivery")

		return err
	}

	return nil
}

// legacyWebhookTaskID namespaces the event ID so it cannot collide with another producer's
// task identity on the shared webhook queue.
//
// The queue is shared with transaction hooks and TypeSense indexing (see the sunset block at
// the foot of this file), and asynq task IDs are unique per QUEUE rather than per task type.
// A bare event ID would be one accidental identifier collision away from silently dropping
// somebody else's task as a duplicate.
//
// Parameters:
//   - eventID string: the outbox event id, already trimmed.
//
// Returns:
//   - string: the namespaced task identity.
func legacyWebhookTaskID(eventID string) string {
	return "legacy-webhook:" + eventID
}

// SendWebhook enqueues a webhook notification task using the Blnk instance's asynq client.
//
// # Prefer EnqueueLegacyWebhookDelivery for dual delivery
//
// This method takes a STRUCT and marshals it, so it cannot preserve bytes it was not given.
// The relay's dual-delivery branch must therefore call EnqueueLegacyWebhookDelivery with the
// stored payload instead: a struct round trip re-orders object keys and re-renders numbers,
// which breaks the byte-for-byte payload-equivalence guarantee while leaving the two bodies
// semantically equal and the difference invisible to review.
//
// It is retained because it is the entry point every existing webhook test exercises, and
// because a caller that legitimately HAS a struct rather than bytes — nothing in the event
// pipeline does — needs one. It also carries no task identity, so it offers no duplicate
// suppression.
//
// NO-OP WHEN UNCONFIGURED — load-bearing, not defensive noise. With no webhook URL
// configured this returns nil without enqueuing, which is why Blnk runs perfectly
// well with no notification sink at all and why the whole existing test suite is
// unaffected by webhook configuration it never sets. The Kafka publisher reproduces
// exactly this contract for an empty KAFKA_BROKERS, for exactly the same reason.
//
// The asynq task type and the queue name are deliberately the same string,
// conf.Queue.WebhookQueue: the handler in cmd/workers.go dispatches on the task type,
// and asynq.Queue routes to the queue. They must stay identical or the enqueued task
// lands on a queue whose mux has no handler for its type and expires unhandled.
//
// Parameters:
// - newWebhook NewWebhook: The webhook notification data to enqueue.
//
// Returns:
// - error: An error if the task could not be enqueued.
func (b *Blnk) SendWebhook(newWebhook NewWebhook) error {
	conf := b.Config()

	if conf.Notification.Webhook.Url == "" {
		return nil
	}

	payload, err := json.Marshal(newWebhook)
	if err != nil {
		return err
	}
	taskOptions := []asynq.Option{asynq.Queue(conf.Queue.WebhookQueue)}
	task := asynq.NewTask(conf.Queue.WebhookQueue, payload, taskOptions...)
	info, err := b.asynqClient.Enqueue(task)
	if err != nil {
		logrus.Error(err, info)
		return err
	}
	return err
}

// ProcessWebhook processes a webhook notification task from the queue.
//
// This is the asynq handler registered against conf.Queue.WebhookQueue in
// initializeWebhookTaskHandlers (cmd/workers.go). It re-reads configuration rather
// than trusting the enqueue-time value, so a URL that has been unconfigured since the
// task was queued results in the task being dropped cleanly instead of delivered to a
// stale endpoint. It shares the pooled b.httpClient with every other outbound call so
// that connections are reused across deliveries.
//
// An unmarshalable task payload is a permanent failure that returning an error cannot
// cure, but the error is returned regardless: asynq's retry and archival machinery is
// how such a task becomes visible to an operator rather than vanishing silently.
//
// Parameters:
// - ctx context.Context: The context for the operation, read for its trace identifiers.
// - task *asynq.Task: The task containing the webhook notification data.
//
// Returns:
// - error: An error if the webhook processing fails.
func (b *Blnk) ProcessWebhook(ctx context.Context, task *asynq.Task) error {
	conf, err := config.Fetch()
	if err != nil {
		return err
	}

	// Ordered before the URL check on purpose. Retirement is the stronger reason not to
	// deliver, and reporting it is more useful to an operator than reporting an absent URL
	// on a deployment that has already moved to Kafka.
	if WebhookSunsetPassed(b.legacyWebhookClock()) {
		logrus.WithFields(retiredLegacyWebhookLogFields(ctx)).Warn(
			"dropping a queued legacy webhook delivery: the webhook deprecation sunset date " +
				"has passed, so the HTTP transport is retired and this task cannot be delivered")

		return nil
	}

	if conf.Notification.Webhook.Url == "" {
		return nil
	}

	// Unmarshal to VALIDATE, then deliver the ORIGINAL BYTES.
	//
	// The decoded value is deliberately discarded. Delivering it would mean marshalling
	// NewWebhook.Payload interface{} again, and that round trip re-orders every object's
	// keys (Go sorts map keys) and re-renders every number through float64 — so the body
	// that reaches the subscriber would differ, byte for byte, from the body recorded in
	// blnk.event_outbox and published to Kafka. The two would stay semantically equal,
	// which is what makes the difference easy to miss and fatal to a byte-level
	// equivalence assertion.
	//
	// The unmarshal is kept because a malformed task payload must still be recognised: it
	// is a permanent failure that no retry can cure, and returning the error is how
	// asynq's archival machinery makes it visible to an operator instead of it vanishing.
	var payload NewWebhook
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		// The ZERO envelope is passed deliberately: nothing was decoded, so there is no
		// event name to report, and legacyWebhookFailureRecord omits the field rather than
		// naming one. A fabricated event_type here would send an investigation to an event
		// that had nothing to do with the failure.
		legacyWebhookFailureRecord(ctx, task, NewWebhook{}, err).
			Error("not a webhook envelope")

		return err
	}

	if err := processHTTPRaw(ctx, task.Payload(), b.httpClient); err != nil {
		// ONE record per failed delivery, and this is it. The envelope is available here, so
		// this is the only place that can name the event alongside the task, queue, retry
		// count and cause — which is what makes a failure diagnosable rather than merely
		// visible. processHTTPRaw deliberately logs nothing.
		legacyWebhookFailureRecord(ctx, task, payload, err).
			Error("legacy webhook delivery failed")

		return err
	}

	return nil
}

// ===== SUNSET (terminal step of the Kafka event-streaming feature) =====
//
// This block is the executable contract for retiring the legacy HTTP transport. It is
// written as an ordered procedure because the order matters: step 1 before step 2
// preserves a contract that step 2 would otherwise destroy, and step 4 is a
// prohibition that step 3 makes tempting.
//
// PRECONDITION. Do none of this until WebhookSunsetPassed (event_sunset.go) answers
// true for the deployed window — that is, until the full 30-day dual-delivery window
// has elapsed and WebhookDualDeliveryActive has answered false ever since. Until then this file must remain compiled
// in and reachable: the dual-delivery payload-equivalence check needs a live second
// transport to compare against, and the 410 Gone behaviour is a runtime decision made
// by that predicate, not a consequence of deleting source.
//
// STEP 1 — RELOCATE FIRST, DELETE SECOND. Two symbols in this file must outlive it,
// because they are not implementation, they are contract:
//
//   - NewWebhook moves to event_outbox.go. Its marshaled form IS the payload carried
//     inside every LedgerEvent, so it survives the transport that named it.
//   - getEventFromStatus moves to event_topics.go. It IS the transaction event-string
//     vocabulary that decides which topic a transaction event is published to.
//
// Every root .go file is package blnk, so both are file moves inside a single package:
// no import changes anywhere, no call site edited, and transaction_execution.go's use
// of getEventFromStatus keeps compiling untouched. Move them, confirm the build, and
// only then continue. Deleting this file first would break the payload contract and
// the event vocabulary at once.
//
// STEP 2 — DELETE. Remove processHTTP, processHTTPRaw, SendWebhook,
// EnqueueLegacyWebhookDelivery, legacyWebhookTaskID, LegacyWebhookRetention and
// ProcessWebhook, then this file, then webhooks_test.go and webhooks_process_test.go.
// Those two test files cover the HTTP transport specifically and have no subject once it
// is gone; the payload and vocabulary behaviours they also touch are by then covered
// where those symbols now live.
//
// Delete the relay's call to EnqueueLegacyWebhookDelivery in the same change, or the
// build breaks at that call site. That is deliberate: the dual-delivery branch and this
// file must go together, and a compile error is a better reminder than a comment.
//
// STEP 3 — UNREGISTER EXACTLY ONE HANDLER. In initializeWebhookTaskHandlers
// (cmd/workers.go) remove the single line that maps the webhook queue to this file's
// handler — mux.HandleFunc(cfg.Queue.WebhookQueue, b.blnk.ProcessWebhook) — and
// nothing else on that mux.
//
// STEP 4 — DO NOT REMOVE THE QUEUE. This is the trap, and it is the most consequential
// prohibition in the whole feature. conf.Queue.WebhookQueue, initializeWebhookQueues,
// initializeWebhookWorkerServer and the webhook asynq server itself must all SURVIVE,
// because the queue is shared infrastructure that predates and outlives this
// transport:
//
//   - initializeWebhookQueues returns two queues, the webhook queue AND the index
//     queue, so deleting it stops search indexing as well.
//   - The mux carries four handlers, of which only ProcessWebhook belongs to this
//     feature; the others are new:hook_execution, the index queue handler and
//     new:index:batch.
//   - internal/hooks/manager.go enqueues PRE_TRANSACTION / POST_TRANSACTION hook work
//     onto conf.Queue.WebhookQueue BY NAME. The /hooks feature is a different feature
//     — synchronous request-time callouts, not asynchronous event notification — and
//     it stays fully functional.
//
// Removing the queue or its worker server would therefore silently disable transaction
// hooks and search indexing, with no compile error to catch it. Remove the mapping
// only.
//
// STEP 5 — DROP DUAL DELIVERY. Remove the dual-delivery branch in event_relay.go so
// the relay publishes to Kafka only, and remove the delivery use of the
// Notification.Webhook configuration block. That branch is more than the enqueue: it
// carries the legacy leg's own state — MarkEventWebhookPending, the webhook_pending
// status, kafka_dispatched_at and webhook_attempts — all of which exist only to keep a
// failed enqueue recoverable during the window, and all of which go with it. Kafka is
// then the sole transport, and the deprecated webhook management routes answer 410 Gone
// through api/middleware/sunset.go under the WebhookSunsetPassed decision.
//
// STEP 6 — REMOVE NO DEPENDENCY. Nothing leaves go.mod at sunset. Deleting
// ProcessWebhook removes a handler registration, not a module: hibiken/asynq,
// hibiken/asynqmon and redis/go-redis all remain required by the transaction queue,
// the hooks subsystem and the index queue. Pruning them would break features that
// merely shared infrastructure with this one.
//
// ===== END SUNSET =====

// retiredLegacyWebhookLogFields describes a task dropped because the sunset has passed.
//
// The retry count is the field worth having. A task at retry 0 was merely sitting in the
// queue when the sunset arrived; a task at retry 4 has been failing against the subscriber
// for the whole of its backoff schedule and would have gone on trying across the boundary.
// The two are different operational stories and the log should not flatten them.
//
// asynq exposes these through the handler context, and each is best-effort: a caller that
// invokes the handler directly — every test of it does — has no asynq context, so the
// lookups fail and the fields are simply omitted rather than reported as zero. A dropped
// task must still be logged when its identity is unknown, so nothing here can fail the drop.
//
// Parameters:
//   - ctx context.Context: the handler context, or any context at all.
//
// Returns:
//   - logrus.Fields: the reason, plus whichever of task ID, queue and retry count are known.
func retiredLegacyWebhookLogFields(ctx context.Context) logrus.Fields {
	fields := logrus.Fields{"reason": legacyWebhookRetiredAtSunsetReason}

	// asynq's context accessors dereference the context without a nil check, so a nil one
	// panics. asynq itself never passes nil, but this function's only job is to describe a
	// drop, and a log helper must never be the reason a worker goroutine dies — the drop has
	// to be reportable even when its identity is not.
	if ctx == nil {
		return fields
	}

	if taskID, ok := asynq.GetTaskID(ctx); ok && taskID != "" {
		fields["task_id"] = sanitizeLogValue(taskID, maxLoggedErrorLength)
	}

	if queue, ok := asynq.GetQueueName(ctx); ok && queue != "" {
		fields["queue"] = sanitizeLogValue(queue, maxLoggedErrorLength)
	}

	if retries, ok := asynq.GetRetryCount(ctx); ok {
		fields["retry_count"] = retries
	}

	return fields
}

// legacyWebhookRetiredAtSunsetReason is the fixed reason a dropped delivery reports.
//
// A constant rather than a sentence written at each site, so an operator grepping for
// retirement finds one string and a test can assert the exact value the log carries.
const legacyWebhookRetiredAtSunsetReason = "legacy webhook obligation retired undelivered: the webhook deprecation sunset date has passed"
