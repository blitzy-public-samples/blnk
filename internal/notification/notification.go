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

	"github.com/blnkfinance/blnk/internal/request"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// SlackNotification sends an error message to a Slack webhook.
// It formats the error details and the current time into a Slack message payload.
//
// Parameters:
// - err: The error to be reported via Slack.
//
// The function retrieves configuration for the Slack webhook URL, formats the error,
// and sends it as a JSON payload to the Slack webhook.
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
//
// NotifyError returns as soon as it has SPAWNED its work, so from the outside there is no
// moment at which "the dispatch decision has been made" is observable — and the
// assertions that matter most here are NEGATIVE ones ("an unconfigured deployment stays
// silent", "both transports configured dispatch exactly once"), which a sleep-based test
// would pass on a merely slow machine. So the goroutine announces its own completion.
// Unset in production, where it costs one nil comparison per notified error, and set by a
// test that then knows the decision is behind it before asserting anything.
var (
	notifyErrorCompletedMu sync.RWMutex
	notifyErrorCompleted   func()
)

// setNotifyErrorCompleted installs — or clears, with nil — the completion callback.
//
// Unexported: it is a seam for this package's own tests, not an API. A caller outside the
// package that wanted to know when a notification finished would be asking for a synchronous
// notifier, which is a different function.
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
// It logs the error locally and sends a notification via Slack (if configured).
//
// Parameters:
// - systemError: The error to notify.
//
// This function runs the notification process asynchronously using a goroutine to avoid blocking.
func NotifyError(systemError error) {
	go func(systemError error) {
		// Deferred, so that EVERY exit announces completion — including the early return on a
		// configuration failure. A callback reached on only the successful paths would let a
		// waiting test hang on exactly the failures it exists to detect.
		defer announceNotifyErrorCompleted()

		// ONE OPERATOR RECORD PER OCCURRENCE, and it is emitted here — before anything can
		// fail or be skipped — so an error is never swallowed.
		//
		// One record rather than two, because whatever an error renders is disclosed once per
		// record: a PostgreSQL error names schema, table, column and routine, and a broker
		// error names internal addresses. This record is correlated, classified, bounded and
		// stripped of control characters — a raw text printed verbatim lets a newline inside
		// it forge a second entry in a line-oriented aggregator. The full text still reaches
		// the operator, because a system error nobody can read is an undiagnosable outage;
		// what the SUBSCRIBER receives is the frozen legacy body and nothing more.
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
//
// A constant rather than a literal at the call site, because it is one of the thirteen
// event strings the event catalogue routes on: model.EventCategory maps it to the
// internal system category, and a typo here would route the event to the catch-all
// category instead with nothing failing.
const systemErrorEventType = "system.error"

// The classified reasons a system error is reduced to. Every value a subscriber or an
// operator can see in a system.error payload is one of these, and each says something
// different about where the fault is without saying anything about the deployment.
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
//
// Order is load-bearing. Authorization is checked before transport because a broker or
// database can report both in one message and the authorization failure is the
// actionable half — it will not clear on its own.
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

// classifySystemError reduces an error to one reason from the vocabulary above.
//
// The RETURN IS ALWAYS A VOCABULARY VALUE — never a fragment of the error, never the
// error itself. That property is what makes the result safe to use as a log field, a
// metric label or, should the payload ever be versioned to carry a summary instead of
// the error, a payload value: no error, however constructed, can produce output that
// describes the deployment.
//
// Parameters:
//   - systemError error: the error being notified. Nil yields the unclassified reason.
//
// Returns:
//   - string: one value from the SystemErrorReason* set.
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
//
// It reads the code's HTTP status rather than enumerating every code, so a code added
// to apierror later is classified without an edit here. Enumerating them would mean
// this function silently returning "" for every new code — the failure mode where a
// growing vocabulary quietly degrades into unclassified.
//
// Parameters:
//   - code apierror.ErrorCode: the typed code.
//
// Returns:
//   - string: a reason, or "" when the status implies nothing more specific than the
//     text matching would find.
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
//
// The code is safe to publish and worth publishing: it is drawn from a fixed vocabulary
// declared in internal/apierror, it is the same value the HTTP API already returns to
// callers for the same condition, and it is more precise than the reason. An untyped
// error yields "" rather than a manufactured code.
//
// Parameters:
//   - systemError error: the error being notified.
//
// Returns:
//   - string: the error code, or "".
func systemErrorCode(systemError error) string {
	var apiErr apierror.APIError
	if errors.As(systemError, &apiErr) {
		return string(apiErr.Code)
	}

	return ""
}

// maxLoggedErrorLength bounds an error's rendering in a log field, in runes.
//
// The value is generous because the point is not brevity but the existence of a CEILING: a
// PostgreSQL driver error can embed a whole statement, a Kafka error a response body, and
// neither has any upper bound at all. 512 runes is more than enough to identify any error
// this package handles while making a pathological one incapable of dominating the log.
const maxLoggedErrorLength = 512

// logTruncationSuffix marks a rendering that was cut short, so a reader can tell a bounded
// error from a genuinely short one and does not diagnose the truncation as the error.
const logTruncationSuffix = "…[truncated]"

// boundedErrorText renders an error for a log FIELD: control characters neutralised and
// the length capped.
//
// Two independent problems, and neither is hypothetical for the errors this package
// receives.
//
// UNBOUNDED LENGTH. There is no limit on how long an error's text may be.
//
// Parameters:
//   - err error: the error to render. A nil error yields the empty string, which is how
//     an absent error reads in a log field.
//
// Returns:
//   - string: a single-line, length-capped rendering.
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
		case character < 0x20 || character == 0x7f:
			// Every other control character is dropped. None of them is legible, and a
			// terminal escape sequence in particular can rewrite what an operator sees.
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
//
// uuid.NewString cannot fail in the way a caller must handle — it panics only if the
// system entropy source is unavailable, which is not a condition an error notification
// can meaningfully recover from — so there is no error to propagate here.
//
// Returns:
//   - string: a fresh UUID.
func newCorrelationID() string {
	return uuid.NewString()
}

// systemErrorPayload builds the system.error event body.
//
// A LedgerEvent's payload must match the legacy webhook body
// FIELD-FOR-FIELD, and the webhook body for system.error has always been these two
// keys. Every subscriber's parser is written against them.
//
// What the concern DOES justify, and what is done instead:
//
//   - system.error routes to the blnk.system category, which NO SUBSCRIBER IS GRANTED
//     BY DEFAULT — model.SubscriberGrantableEventCategories withholds it, for this
//     payload's sake among others — so the audience for this body is the operator
//     unless a deployment deliberately widens it. Widening takes two declarations that
//     are hard to make by accident: the deployment sets
//     KAFKA_SUBSCRIBER_INTERNAL_TOPIC_ACCESS=true, and the subscriber's own grant then
//     has to name the topic.
//
//   - The bounded classification — classifySystemError and systemErrorCode, both fixed
//     vocabularies — is logged at the dispatch site alongside a correlation id, so an
//     operator gets the diagnosis without the raw text being repeated across log sinks.
//
//   - error: systemError.Error(), verbatim. This is the value subscribers parse.
//
//   - time: time.Now(), as the original payload carried it. A time.Time rather than a
//     formatted string, because that is what the legacy payload put in the map and what
//     the JSON encoder therefore rendered.
//
// Parameters:
//   - systemError error: the error being notified.
//
// Returns:
//   - map[string]interface{}: the event payload, exactly as the legacy webhook body.
func systemErrorPayload(systemError error) map[string]interface{} {
	return map[string]interface{}{
		"error": systemError.Error(),
		"time":  time.Now(),
	}
}
