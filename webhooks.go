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

// This file is the LEGACY HTTP webhook transport. It is retained deliberately, and only
// for the duration of the 30-day dual-delivery window that accompanies the move to
// Kafka event streaming. Nothing in it is new work; everything in it is frozen.

// legacyWebhookPrivateDestinationWarning makes the operator's private-destination
// assertion appear in the log exactly once per process.
var legacyWebhookPrivateDestinationWarning sync.Once

// errLegacyWebhookRedirect is the sentinel every refused redirect wraps.
//
// A sentinel rather than a formatted string so a test can assert the REASON a delivery
// failed with errors.Is instead of matching prose, and so a future caller can
// distinguish "the endpoint tried to redirect us" from a transport failure it should
// retry differently.
var errLegacyWebhookRedirect = errors.New("legacy webhook delivery refused to follow a redirect")

// errLegacyWebhookDestination is the sentinel every refused destination wraps.
var errLegacyWebhookDestination = errors.New("legacy webhook destination is not permitted")

// legacyWebhookAllowsPrivateDestination reports whether the operator has asserted that
// the webhook destination is on a network they own.
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
//   - req *http.Request: the request the client is about to make to the redirect
//     target.
//   - via []*http.Request: the requests already made, so the hop count can be reported.
//
// Returns:
//   - error: always non-nil, wrapping errLegacyWebhookRedirect.
func refuseLegacyWebhookRedirect(req *http.Request, via []*http.Request) error {
	target := "unknown"
	if req != nil && req.URL != nil {
		// Scheme and host only. The path can carry a subscriber's own identifiers, and the
		// whole value is third-party text, so it is bounded and stripped of control
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

// guardLegacyWebhookDial is the net.Dialer Control hook for the legacy transport. It
// runs after DNS resolution and before connect, and refuses an address Blnk must not
// reach.
//
// This is the only point at which the address actually being connected to is known. A
// check on the URL text judges a name; this judges the answer.
//
// Parameters:
//   - network string: the dial network, e.g. "tcp4".
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

// validateLegacyWebhookDestination applies the URL-text half of the destination policy
// to the configured endpoint.
//
// It is the cheap, early half. It rejects the schemes and the obviously-internal hosts
// before a request is built, so the common misconfiguration produces one clear error
// instead of a dial failure an operator has to interpret.
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
// The wire contract it implements is unchanged by the move to Kafka and is asserted end
// to end by webhooks_process_test.go, which reconstructs the signature from what the
// receiver was handed:
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

	// No identity: this path was handed a STRUCT, so there is no outbox row and no event id
	// behind it. The header is omitted rather than invented.
	return processHTTPRaw(ctx, "", payloadBytes, client)
}

// processHTTPRaw sends an already-serialised webhook body, signing and posting the
// exact bytes it was handed.
//
// It returns the failure, status code included, and its callers record it once. At the
// pipeline's 500-events-per-second target that duplicate is not a second opinion, it is
// half the log.
//
// Parameters:
//   - ctx context.Context: bounds the request. Cancelling it abandons the delivery.
//   - payloadBytes []byte: the body to send, verbatim. Never inspected, never
//     rewritten.
//   - client *http.Client: the pooled client to send with.
//
// Returns:
//   - error: a configuration, request-construction, transport or non-2xx failure.
func processHTTPRaw(ctx context.Context, eventID string, payloadBytes []byte, client *http.Client) error {
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

	// THE IDENTITY, when the caller has one. Set before the signature so the ordering
	// reads as "describe the delivery, then sign the body", and set before the configured
	// headers so applyConfiguredWebhookHeaders can refuse an override of it.
	if trimmedEventID := strings.TrimSpace(eventID); trimmedEventID != "" {
		req.Header.Set(LegacyWebhookEventIDHeader, trimmedEventID)
	}

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
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxWebhookResponseDrainBytes))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Returning the error lets asynq retry the delivery; swallowing it would permanently
		// drop the webhook on receiver-side failures. The status is carried IN the error
		// rather than logged here, so the caller's single record can name it alongside the
		// event, task and queue it belongs to.
		return fmt.Errorf("webhook delivery failed with status %d", resp.StatusCode)
	}

	return nil
}

// transportOwnedWebhookHeaders are the headers processHTTPRaw computes for itself, in
// the canonical form net/http stores them under.
//
// They are listed rather than derived because each one carries a guarantee the delivery
// makes about ITSELF, which no amount of configuration can be allowed to restate:
//
//   - X-Blnk-Signature and X-Blnk-Timestamp are the HMAC over timestamp + "." + body and the
//     timestamp it is computed against. They are the only evidence a subscriber has that the
//     body came from this deployment.
//   - Content-Type describes a body this function marshalled. The body is always JSON, and a
//     receiver told otherwise mis-parses a payload that is perfectly well formed.
//   - Content-Length is set by net/http from the body it is actually sending; a configured
//     value would either truncate the request or make it hang waiting for bytes.
var transportOwnedWebhookHeaders = map[string]struct{}{
	textproto.CanonicalMIMEHeaderKey("Content-Type"):     {},
	textproto.CanonicalMIMEHeaderKey("Content-Length"):   {},
	textproto.CanonicalMIMEHeaderKey("X-Blnk-Signature"): {},
	textproto.CanonicalMIMEHeaderKey("X-Blnk-Timestamp"): {},
	// The event identity, for the same reason as the signature: it is a statement the
	// transport makes about THIS delivery, and a receiver deduplicating on it must be able
	// to trust it. A configured constant here would collapse every event onto one identity
	// and a receiver would discard all but the first as duplicates — silently, and
	// permanently.
	textproto.CanonicalMIMEHeaderKey(LegacyWebhookEventIDHeader): {},
}

// LegacyWebhookEventIDHeader carries the outbox event id to the receiver of a legacy
// HTTP delivery.
//
// The body is FROZEN. Its bytes are asserted equal to the bytes published to Kafka, and
// the payload-preservation guarantee is that a subscriber's existing parser works
// unchanged.
const LegacyWebhookEventIDHeader = "X-Blnk-Event-Id"

// applyConfiguredWebhookHeaders copies the operator's configured headers onto an
// outgoing request, refusing the ones the transport owns.
//
// Only the four headers the transport computes are refused. A configured Authorization,
// X-Tenant or User-Agent is exactly what this setting is for — the receiver's own
// requirements — and filtering by prefix or by an allowlist would break that.
//
// Parameters:
//   - header http.Header: the outgoing request's headers, already carrying the computed
//     ones.
//   - configured map[string]string: the operator's configured headers.
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

// maxWebhookResponseDrainBytes bounds how much of a webhook receiver's response body is
// read before the connection is returned to the idle pool.
const maxWebhookResponseDrainBytes = 64 << 10

// unsignedWebhookWarningInterval is the shortest gap between two warnings about the
// same unconfigured signing key.
const legacyWebhookTaskIDNamespace = "legacy-webhook:"

const unsignedWebhookWarningInterval = 10 * time.Minute

// rateLimitedWarning admits at most one occurrence of a repeating condition per
// interval and counts the ones it withholds.
type rateLimitedWarning struct {
	mu sync.Mutex

	// interval is the minimum gap between emitted warnings. A non-positive interval emits
	// every occurrence, which is the pre-rate-limit behaviour and is useful in a test that
	// wants to observe every one.
	interval time.Duration

	// now is the clock, injectable so a test can advance time without sleeping. Never nil
	// in the package-level instances; admit falls back to time.Now if it is, so a
	// zero-value struct is still usable.
	now func() time.Time

	emitted     bool
	lastEmitted time.Time
	suppressed  uint64
}

// admit records one occurrence of the condition and decides whether it should be
// logged.
//
// Returns:
//   - bool: true when the caller should log this occurrence.
//   - uint64: when logging, how many occurrences were withheld since the previous
//     logged one, not counting this one.
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
// deployment's configuration, not any one delivery, so every delivery site must share
// one budget or the limit means nothing. It is created here rather than lazily so there
// is no initialisation race between concurrent workers.
var unsignedWebhookWarning = &rateLimitedWarning{
	interval: unsignedWebhookWarningInterval,
	now:      time.Now,
}

// warnWebhookSentUnsigned reports an unsigned delivery at most once per
// unsignedWebhookWarningInterval, saying how many deliveries the suppressed interval
// covered.
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

// legacyWebhookFailureRecord assembles the one structured record a failed legacy
// delivery produces, naming what failed as precisely as the available identity allows.
//
// The context getters return ok=false for a context asynq did not create, and every
// such field is then OMITTED. A record that says retry_count=0 for a context that never
// carried one is worse than a record that says nothing: it reads as a first attempt.
//
// Parameters:
//   - ctx context.Context: the asynq handler context, read for identity only.
//   - task *asynq.Task: the task being processed. Read for its type and payload length.
//   - envelope NewWebhook: the decoded envelope, or the zero value when it could not be
//     decoded.
//   - cause error: the failure. Never nil at any call site; a nil cause simply
//     contributes no error field.
//
// Returns:
//   - *logrus.Entry: the assembled record, ready for the caller to give a message.
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

// legacyWebhookEventIDFromContext recovers the outbox event id for the delivery being
// handled.
//
// Parameters:
//   - ctx context.Context: the handler context asynq supplied. May be nil.
//
// Returns:
//   - string: the event id, or "" when this task did not come from the relay.
func legacyWebhookEventIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}

	taskID, ok := asynq.GetTaskID(ctx)
	if !ok {
		return ""
	}

	return legacyWebhookEventID(taskID)
}

// LegacyWebhookRetention is how long a completed legacy-delivery task is kept in Redis.
//
// Because the cost is unbounded where the alternative is free. Retention keeps the
// completed task, not a token: covering the full thirty-day dual-delivery window would
// hold every legacy delivery of those thirty days in Redis, which at the pipeline's
// target rate is on the order of a billion tasks.
const LegacyWebhookRetention = 24 * time.Hour

// EnqueueLegacyWebhookDelivery enqueues the legacy HTTP delivery of ONE outbox event,
// carrying the stored bytes verbatim and using the event ID as the task's identity.
//
// So this path never decodes. It validates that the bytes are a well-formed legacy
// envelope, and then carries them unchanged all the way to the socket.
//
// Parameters:
//   - eventID string: the outbox row's event_id, which is the task's identity.
//   - body []byte: the stored legacy webhook body, carried verbatim.
//
// Returns:
//   - error: nil when the task is enqueued, and nil when an identical task was already
//     enqueued.
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

	// Validation, NOT transformation.
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
			// post-condition the caller needs — this event's webhook leg is queued exactly once
			// — holds, so this is success.
			logrus.WithFields(logrus.Fields{
				"event_id": eventID,
				"task_id":  legacyWebhookTaskID(eventID),
			}).Debug("legacy webhook delivery for this event is already enqueued; the duplicate was suppressed")

			return nil
		}

		withLoggableCause(logrus.WithField("event_id", eventID), err).
			Error("could not enqueue legacy webhook delivery")

		return err
	}

	return nil
}

// legacyWebhookTaskID namespaces the event ID so it cannot collide with another
// producer's task identity on the shared webhook queue.
//
// Parameters:
//   - eventID string: the outbox event id, already trimmed.
//
// Returns:
//   - string: the namespaced task identity.
func legacyWebhookTaskID(eventID string) string {
	return "legacy-webhook:" + eventID
}

// SendWebhook enqueues a webhook notification task using the Blnk instance's asynq
// client.
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
	// The unmarshal is kept because a malformed task payload must still be recognised: it
	// is a permanent failure that no retry can cure, and returning the error is how
	// asynq's archival machinery makes it visible to an operator instead of it vanishing.
	var payload NewWebhook
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		// The ZERO envelope is passed deliberately: nothing was decoded, so there is no event
		// name to report, and legacyWebhookFailureRecord omits the field rather than naming
		// one. A fabricated event_type here would send an investigation to an event that had
		// nothing to do with the failure.
		legacyWebhookFailureRecord(ctx, task, NewWebhook{}, err).
			Error("not a webhook envelope")

		return err
	}

	// The identity travels as the TASK ID rather than in the body, so it is recovered from
	// there — the same inverse pair that lets a failure log name the event. A task from
	// another producer on the shared queue yields the empty string and the header is
	// omitted.
	if err := processHTTPRaw(ctx, legacyWebhookEventIDFromContext(ctx), task.Payload(), b.httpClient); err != nil {
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
// preserves a contract that step 2 would otherwise destroy, and step 4 is a prohibition
// that step 3 makes tempting.
//
// THE OBLIGATION IS PUBLISHED AND ENFORCED, not merely recorded here. This procedure
// has two counterparts, and all three describe one release:
//
//   - docs/webhook-to-kafka-migration.md carries the same release as an operator-facing
//     DELETION CHECKLIST — the artifacts, where each lives, and the preserve half — because
//     the party who performs it reads the migration guide, not this file.
//   - TestWebhookTerminalRelease_ChecklistMatchesTheSurface (event_sunset_test.go) holds that
//     checklist to this tree in both directions: a row naming something that no longer exists
//     fails, and an exported symbol declared here that the checklist does not name fails. So
//     the release cannot be performed without editing the checklist, and the checklist cannot
//     rot while the transport is still compiled in.
//
// STEP 6 — REMOVE NO DEPENDENCY. Nothing leaves go.mod at sunset.
//
// ===== END SUNSET =====

// retiredLegacyWebhookLogFields describes a task dropped because the sunset has passed.
//
// The retry count is the field worth having. A task at retry 0 was merely sitting in
// the queue when the sunset arrived; a task at retry 4 has been failing against the
// subscriber for the whole of its backoff schedule and would have gone on trying across
// the boundary.
//
// Parameters:
//   - ctx context.Context: the handler context, or any context at all.
//
// Returns:
//   - logrus.Fields: the reason, plus whichever of task ID, queue and retry count are
//     known.
func retiredLegacyWebhookLogFields(ctx context.Context) logrus.Fields {
	fields := logrus.Fields{"reason": legacyWebhookRetiredAtSunsetReason}

	// asynq's context accessors dereference the context without a nil check, so a nil one
	// panics. asynq itself never passes nil, but this function's only job is to describe a
	// drop, and a log helper must never be the reason a worker goroutine dies — the drop
	// has to be reportable even when its identity is not.
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
