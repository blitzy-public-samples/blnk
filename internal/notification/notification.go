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

package notification

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/blnkfinance/blnk/internal/request"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// SlackNotification sends an error message to a Slack webhook.
//
// Parameters:
// - err: The error to be reported via Slack.
func SlackNotification(err error) {
	// Build the Slack message payload with typed structs and json.Marshal so
	// error text containing quotes, backslashes, or newlines is safely
	// encoded instead of corrupting (or injecting into) the JSON template.
	type slackText struct {
		Type  string `json:"type"`
		Text  string `json:"text"`
		Emoji bool   `json:"emoji,omitempty"`
	}
	type slackBlock struct {
		Type   string      `json:"type"`
		Text   *slackText  `json:"text,omitempty"`
		Fields []slackText `json:"fields,omitempty"`
	}
	payloadStruct := struct {
		Blocks []slackBlock `json:"blocks"`
	}{
		Blocks: []slackBlock{
			{
				Type: "header",
				Text: &slackText{Type: "plain_text", Text: "Error From Blnk 🐞", Emoji: true},
			},
			{
				Type:   "section",
				Fields: []slackText{{Type: "mrkdwn", Text: fmt.Sprintf("*Error:*\n%v", err.Error())}},
			},
			{
				Type:   "section",
				Fields: []slackText{{Type: "mrkdwn", Text: fmt.Sprintf("*Time:*\n%v", time.Now().Format(time.RFC822))}},
			},
		},
	}

	encoded, err := json.Marshal(payloadStruct)
	if err != nil {
		logrus.Error(err)
		return
	}
	data := json.RawMessage(encoded)

	// Fetch the configuration, including the Slack webhook URL
	conf, err := config.Fetch()
	if err != nil {
		logrus.Error(err)
		return
	}

	// Convert the Slack message to a JSON request payload
	payload, err := request.ToJsonReq(&data)
	if err != nil {
		logrus.Error(err)
		return
	}

	// Create an HTTP request to send the Slack notification
	req, err := http.NewRequest("POST", conf.Notification.Slack.WebhookUrl, payload)
	if err != nil {
		logrus.Error(err)
		return
	}

	// Send the request and handle the response
	var response map[string]interface{}
	_, err = request.Call(req, &response)
	if err != nil {
		logrus.Error(err)
	}
}

// WebhookSender defines a function signature for sending webhooks.
type WebhookSender func(event string, payload interface{}) error

var (
	webhookSenderMu sync.RWMutex
	webhookSender   WebhookSender
)

// RegisterWebhookSender registers a function to handle webhook sending.
func RegisterWebhookSender(sender WebhookSender) {
	webhookSenderMu.Lock()
	webhookSender = sender
	webhookSenderMu.Unlock()
}

// getWebhookSender returns the currently registered webhook sender, if any.
func getWebhookSender() WebhookSender {
	webhookSenderMu.RLock()
	defer webhookSenderMu.RUnlock()
	return webhookSender
}

// notifyErrorCompleted is called, if set, when NotifyError's goroutine has finished —
// on every path, including the ones that dispatch nothing.
var (
	notifyErrorCompletedMu sync.RWMutex
	notifyErrorCompleted   func()
)

// setNotifyErrorCompleted installs — or clears, with nil — the completion callback.
func setNotifyErrorCompleted(callback func()) {
	notifyErrorCompletedMu.Lock()
	notifyErrorCompleted = callback
	notifyErrorCompletedMu.Unlock()
}

// announceNotifyErrorCompleted runs the completion callback if one is installed.
func announceNotifyErrorCompleted() {
	notifyErrorCompletedMu.RLock()
	callback := notifyErrorCompleted
	notifyErrorCompletedMu.RUnlock()

	if callback != nil {
		callback()
	}
}

// NotifyError sends an error notification through the configured notification system.
//
// Parameters:
// - systemError: The error to notify.
func NotifyError(systemError error) {
	go func(systemError error) {
		// Deferred, so that EVERY exit announces completion — including the early return on a
		// configuration failure. A callback reached on only the successful paths would let a
		// waiting test hang on exactly the failures it exists to detect.
		defer announceNotifyErrorCompleted()

		// NOT EVERY ERROR IS A FAULT, AND THE ONES THAT ARE NOT MUST NOT BE PUBLISHED.
		//
		// This gate is the difference between an incident channel that means something and one
		// that is ignored. A reused transaction reference is the case it was written for: the
		// ledger DETECTED a duplicate and REFUSED it, which is the reference mechanism working,
		// and every route to it is a case the system handles correctly —
		//
		//   - a coalesced follower whose transaction a leader already committed as part of a
		//     batch, which one batch of 2,000 children produces up to 2,000 of;
		//   - an asynq redelivery of a task whose handler had already committed, which
		//     at-least-once delivery guarantees will happen;
		//   - a client resubmitting a reference, which the API answers with 409 and
		//     TXN_DUPLICATE_REFERENCE.
		//
		// Reported as a system error it cost, per occurrence, one operator-facing ERROR line
		// and one system.error event — and since events became outbox-backed, that event is a
		// row in blnk.event_outbox for the relay to publish. A load run measured 1,763 error
		// lines and 1,366 pending system.error events, so the ledger was paging its operators
		// and loading its own event pipeline in proportion to how often it correctly rejected a
		// duplicate.
		//
		// THE GATE IS HERE, AT THE NOTIFICATION LAYER, RATHER THAN AT EACH CALLER, and that is
		// deliberate: the callers are spread across the transaction pipeline, some of them in
		// files this change must not modify, and every one of them would have to make the same
		// judgement correctly and keep making it. What "is a system error" means belongs to the
		// package that defines system errors.
		//
		// Nothing is silenced. The error is still RETURNED by the code that produced it and
		// still acted upon — 409 at the API, an acknowledgement in the worker, a rejected
		// transaction in the ledger — and it is recorded here at info level so a single
		// occurrence remains traceable. What stops is the paging and the publishing.
		if reason, expected := expectedCondition(systemError); expected {
			logrus.WithFields(logrus.Fields{
				"event":  systemErrorEventType,
				"reason": reason,
				"error":  boundedErrorText(systemError),
			}).Info(
				"an expected condition was reported through the system-error channel; it is " +
					"recorded here and deliberately not published as a system error",
			)

			return
		}

		// ONE OPERATOR RECORD PER OCCURRENCE, and it is emitted here — before anything can
		// fail or be skipped — so an error is never swallowed.
		correlationID := newCorrelationID()
		operatorRecord := logrus.WithFields(logrus.Fields{
			"correlation_id": correlationID,
			"event":          systemErrorEventType,
			"error_code":     systemErrorCode(systemError),
			"reason":         classifySystemError(systemError),
			"error":          boundedErrorText(systemError),
		})
		operatorRecord.Error(
			"a system error occurred; this is the only record carrying its text, and its " +
				"correlation_id is what ties it to the system.error event subscribers receive",
		)

		// Fetch the configuration
		conf, err := config.Fetch()
		if err != nil {
			// Correlated to the record above and bounded for the same reasons, so a
			// configuration failure that prevents notification is still traceable to the
			// error it was trying to report.
			operatorRecord.WithField("config_error", boundedErrorText(err)).Error(
				"the configuration could not be read, so the system error above was neither " +
					"sent to Slack nor published as an event",
			)

			return
		}

		// THE DURABLE RECORD IS ATTEMPTED FIRST, and the optional side channel second.
		sender := getWebhookSender()
		if sender != nil && (len(conf.Kafka.Brokers) > 0 || conf.Notification.Webhook.Url != "") {
			// THE PAYLOAD IS THE FROZEN LEGACY CONTRACT: {"error": <text>, "time": <now>}, and
			// nothing else. See systemErrorPayload for why it must not be reshaped — the
			// requirement is that a subscriber's existing parser keeps working when only the
			// transport changes, so neither a new key nor a substituted error value belongs
			// here. The bounded, classified diagnosis and the correlation id are on the operator
			// record above instead, which is where narrowing costs nothing.
			payload := systemErrorPayload(systemError)
			if err := sender(systemErrorEventType, payload); err != nil {
				// A DISTINCT FAILURE, so it gets its own record rather than being folded into the
				// one above — the system error happened, and separately the attempt to report it
				// did not. Both carry the correlation id, so the pair reads as one story.
				operatorRecord.WithField("sender_error", boundedErrorText(err)).Error(
					"the system.error event could not be published; the system error above " +
						"went unreported to subscribers",
				)
			}
		}

		// Slack LAST, and unconditionally reached: it is an operator convenience, so its
		// outcome cannot affect the event above and its latency can no longer delay it. A
		// failure inside it is logged by SlackNotification itself.
		if conf.Notification.Slack.WebhookUrl != "" {
			SlackNotification(systemError)
		}
	}(systemError)
}

// systemErrorEventType is the event name system.error is published under.
const systemErrorEventType = "system.error"

// The classified reasons a system error is reduced to, FOR THE OPERATOR LOG.
//
// The classification bounds what the LOG says: each value states something about where
// the fault is without saying anything about the deployment. It does NOT bound what a
// subscriber sees. systemErrorPayload carries systemError.Error() verbatim, because the
// payload must match the legacy webhook body field for field, so a subscriber granted the
// system topic reads the original error text — including whatever a driver or a client
// library put in it. Classify the log; treat the payload as disclosure, and grant the
// system topic as the entitlement decision it is.
const (
	// SystemErrorReasonPersistence — the database rejected or could not serve a
	// statement. Includes constraint violations and connectivity alike, because from
	// outside the process they need the same first question: is the database healthy?
	SystemErrorReasonPersistence = "persistence_failure"

	// SystemErrorReasonTransport — an outbound dependency was unreachable or dropped
	// the connection: Kafka, Redis, TypeSense, an HTTP callout.
	SystemErrorReasonTransport = "transport_failure"

	// SystemErrorReasonAuthorization — a credential was refused or a grant was
	// missing. Distinguished from transport because it will not clear on its own.
	SystemErrorReasonAuthorization = "authorization_failure"

	// SystemErrorReasonTimeout — an operation exceeded its deadline or its context
	// was cancelled.
	SystemErrorReasonTimeout = "timeout"

	// SystemErrorReasonValidation — input was rejected. A caller's fault rather than
	// the system's, and worth separating for exactly that reason.
	SystemErrorReasonValidation = "validation_failure"

	// SystemErrorReasonConfiguration — the deployment is misconfigured.
	SystemErrorReasonConfiguration = "configuration_error"

	// SystemErrorReasonUnclassified — the error matched nothing above. The full text
	// is in the log line correlated by correlation_id, and this value says so rather
	// than guessing.
	SystemErrorReasonUnclassified = "unclassified"
)

// systemErrorSignatures maps a lowercase substring of an error's text to the reason it
// implies, most specific first.
var systemErrorSignatures = []struct {
	signature string
	reason    string
}{
	{"authoriz", SystemErrorReasonAuthorization},
	{"authentic", SystemErrorReasonAuthorization},
	{"permission denied", SystemErrorReasonAuthorization},
	{"sasl", SystemErrorReasonAuthorization},
	{"deadline exceeded", SystemErrorReasonTimeout},
	{"context canceled", SystemErrorReasonTimeout},
	{"timeout", SystemErrorReasonTimeout},
	{"timed out", SystemErrorReasonTimeout},
	{"constraint", SystemErrorReasonPersistence},
	{"pq:", SystemErrorReasonPersistence},
	{"sql", SystemErrorReasonPersistence},
	{"database", SystemErrorReasonPersistence},
	{"duplicate key", SystemErrorReasonPersistence},
	{"connection refused", SystemErrorReasonTransport},
	{"broken pipe", SystemErrorReasonTransport},
	{"no such host", SystemErrorReasonTransport},
	{"reset by peer", SystemErrorReasonTransport},
	{"unreachable", SystemErrorReasonTransport},
	{"eof", SystemErrorReasonTransport},
	{"invalid", SystemErrorReasonValidation},
	{"required", SystemErrorReasonValidation},
	{"must be", SystemErrorReasonValidation},
	{"config", SystemErrorReasonConfiguration},
	{"not configured", SystemErrorReasonConfiguration},
}

// SystemErrorReasonDuplicateReference names the one expected condition currently gated out
// of the system-error channel.
const SystemErrorReasonDuplicateReference = "duplicate_reference"

// expectedConditionSignatures are error texts that describe the ledger WORKING, not failing.
//
// Matched on text because the callers construct these with fmt.Errorf rather than a typed
// error, and several of them are in files this change must not modify — so a typed sentinel
// cannot be introduced at the source. The signatures are therefore both required: "reference"
// alone would swallow unrelated reference failures, and "already been used" alone would match
// any resource. Both together identify exactly the duplicate-reference refusal, whose wording
// is fixed by the two callers that produce it and by IsDuplicateReferenceError, which
// classifies the same condition from the same text.
var expectedConditionSignatures = []struct {
	all    []string
	reason string
}{
	{all: []string{"reference", "already been used"}, reason: SystemErrorReasonDuplicateReference},
}

// expectedCondition reports whether an error describes an expected condition rather than a
// system fault, and names it.
func expectedCondition(systemError error) (string, bool) {
	if systemError == nil {
		return "", false
	}

	lowered := strings.ToLower(systemError.Error())

	for _, candidate := range expectedConditionSignatures {
		matched := true

		for _, fragment := range candidate.all {
			if !strings.Contains(lowered, fragment) {
				matched = false

				break
			}
		}

		if matched {
			return candidate.reason, true
		}
	}

	return "", false
}

// classifySystemError reduces an error to one reason from the vocabulary above.
func classifySystemError(systemError error) string {
	if systemError == nil {
		return SystemErrorReasonUnclassified
	}

	var apiErr apierror.APIError
	if errors.As(systemError, &apiErr) {
		if reason := reasonForErrorCode(apiErr.Code); reason != "" {
			return reason
		}
	}

	lowered := strings.ToLower(systemError.Error())
	for _, candidate := range systemErrorSignatures {
		if strings.Contains(lowered, candidate.signature) {
			return candidate.reason
		}
	}

	return SystemErrorReasonUnclassified
}

// reasonForErrorCode classifies a typed apierror code.
func reasonForErrorCode(code apierror.ErrorCode) string {
	switch apierror.StatusForCode(code) {
	case http.StatusUnauthorized, http.StatusForbidden:
		return SystemErrorReasonAuthorization
	case http.StatusBadRequest, http.StatusUnprocessableEntity, http.StatusConflict:
		return SystemErrorReasonValidation
	case http.StatusRequestTimeout, http.StatusGatewayTimeout:
		return SystemErrorReasonTimeout
	case http.StatusServiceUnavailable, http.StatusBadGateway:
		return SystemErrorReasonTransport
	default:
		return ""
	}
}

// systemErrorCode returns the typed apierror code an error carries, or "" when it
// carries none.
func systemErrorCode(systemError error) string {
	var apiErr apierror.APIError
	if errors.As(systemError, &apiErr) {
		return string(apiErr.Code)
	}

	return ""
}

// maxLoggedErrorLength bounds an error's rendering in a log field, in runes.
const maxLoggedErrorLength = 512

// logTruncationSuffix marks a rendering that was cut short, so a reader can tell a bounded
// error from a genuinely short one and does not diagnose the truncation as the error.
const logTruncationSuffix = "…[truncated]"

// zeroWidthNonJoiner and zeroWidthJoiner are the two Unicode format characters
// boundedErrorText keeps rather than drops: both carry orthographic meaning — in Persian,
// Arabic and Indic scripts, and in emoji sequences — so removing them would corrupt a
// legitimate message instead of neutralising a hostile one.
const (
	zeroWidthNonJoiner = '\u200C'
	zeroWidthJoiner    = '\u200D'
)

// boundedErrorText renders an error for a log FIELD: control characters neutralised and
// the length capped.
func boundedErrorText(err error) string {
	if err == nil {
		return ""
	}

	var builder strings.Builder
	builder.Grow(len(err.Error()))

	for _, character := range err.Error() {
		switch {
		case character == '\n' || character == '\r' || character == '\t':
			// Replaced with a space rather than dropped: removing them would run words
			// together and make a multi-line error harder to read than it needs to be.
			builder.WriteRune(' ')
		case unicode.IsControl(character):
			// Every other control character is dropped. None of them is legible, and a
			// terminal escape sequence in particular can rewrite what an operator sees. The
			// range includes the C1 controls, where U+009B is a CSI introducer on its own — a
			// test that stopped at DEL left that introducer in the field.
		case character == zeroWidthNonJoiner || character == zeroWidthJoiner:
			// Kept: these two format characters carry orthographic meaning, so dropping them
			// would corrupt a legitimate message rather than sanitise a hostile one.
			builder.WriteRune(character)
		case unicode.Is(unicode.Cf, character):
			// Dropped for the same reason as a control character, by a different mechanism:
			// U+202E RIGHT-TO-LEFT OVERRIDE reverses the rendering of everything after it, so
			// an error string carrying one rewrites how the rest of the log entry reads, and
			// U+200B and U+FEFF are invisible, so they hide differences between two entries.
		default:
			builder.WriteRune(character)
		}
	}

	rendered := []rune(builder.String())
	if len(rendered) <= maxLoggedErrorLength {
		return string(rendered)
	}

	return string(rendered[:maxLoggedErrorLength]) + logTruncationSuffix
}

// newCorrelationID returns the identifier that ties a system.error event to the log
// line carrying the full error.
func newCorrelationID() string {
	return uuid.NewString()
}

// systemErrorPayload builds the system.error event body.
func systemErrorPayload(systemError error) map[string]interface{} {
	return map[string]interface{}{
		"error": systemError.Error(),
		"time":  time.Now(),
	}
}
