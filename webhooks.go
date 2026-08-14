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
var errLegacyWebhookRedirect = errors.New("legacy webhook delivery refused to follow a redirect")

// errLegacyWebhookDestination is the sentinel every refused destination wraps.
var errLegacyWebhookDestination = errors.New("legacy webhook destination is not permitted")

// legacyWebhookAllowsPrivateDestination reports whether the operator has asserted that
// the webhook destination is on a network they own.
func legacyWebhookAllowsPrivateDestination() bool {
	conf, err := config.Fetch()
	if err != nil || conf == nil {
		return false
	}

	return conf.Notification.Webhook.AllowPrivateDestination
}

// refuseLegacyWebhookRedirect is the http.Client CheckRedirect hook for the legacy
// transport. It never permits a redirect.
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
const LegacyWebhookEventIDHeader = "X-Blnk-Event-Id"

// applyConfiguredWebhookHeaders copies the operator's configured headers onto an
// outgoing request, refusing the ones the transport owns.
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
func legacyWebhookEventID(taskID string) string {
	eventID, found := strings.CutPrefix(taskID, legacyWebhookTaskIDNamespace)
	if !found {
		return ""
	}

	return strings.TrimSpace(eventID)
}

// legacyWebhookEventIDFromContext recovers the outbox event id for the delivery being
// handled.
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
const LegacyWebhookRetention = 24 * time.Hour

// EnqueueLegacyWebhookDelivery enqueues the legacy HTTP delivery of ONE outbox event,
// carrying the stored bytes verbatim and using the event ID as the task's identity.
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

// retiredLegacyWebhookLogFields describes a task dropped because the sunset has passed.
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
const legacyWebhookRetiredAtSunsetReason = "legacy webhook obligation retired undelivered: the webhook deprecation sunset date has passed"
