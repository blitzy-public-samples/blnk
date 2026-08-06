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

// NotifyError sends an error notification through the configured notification system.
// It logs the error locally and sends a notification via Slack (if configured).
//
// Parameters:
// - systemError: The error to notify.
//
// This function runs the notification process asynchronously using a goroutine to avoid blocking.
func NotifyError(systemError error) {
	go func(systemError error) {
		// Log the error locally using logrus
		logrus.Error(systemError)

		// Fetch the configuration
		conf, err := config.Fetch()
		if err != nil {
			logrus.Error(err)
			return
		}

		// If Slack is configured, send the error notification to Slack
		if conf.Notification.Slack.WebhookUrl != "" {
			SlackNotification(systemError)
		}

		// Dispatch system.error whenever EITHER event transport is configured: Kafka
		// brokers or the legacy webhook URL. This sender is system.error's only route
		// into the event pipeline — it is the one event type with no direct producer
		// call site — so gating on the webhook URL alone would drop it entirely on a
		// Kafka-only deployment, which is the intended end state once webhooks are
		// retired. With neither configured nothing is attempted, preserving the
		// historic no-op-when-unconfigured behaviour. A non-empty broker list means at
		// least one dialable address: setKafkaDefaults normalizes blank entries away.
		sender := getWebhookSender()
		if sender != nil && (len(conf.Kafka.Brokers) > 0 || conf.Notification.Webhook.Url != "") {
			// The payload is SANITIZED and the raw error is NOT in it. See
			// sanitizedSystemErrorPayload for what replaces it and why.
			//
			// The correlation ID is generated first and logged immediately below, because
			// it is worthless in the payload unless the same value is in the log: it is
			// the only thing that lets an operator holding a system.error event find the
			// full error text. The log line is emitted even when the publish then fails,
			// so the correlation never depends on delivery succeeding.
			correlationID := newCorrelationID()
			logrus.WithError(systemError).WithFields(logrus.Fields{
				"correlation_id": correlationID,
				"event":          systemErrorEventType,
				"error_code":     systemErrorCode(systemError),
				"reason":         classifySystemError(systemError),
			}).Error("publishing a sanitized system.error event; the full error is on this line only")

			payload := sanitizedSystemErrorPayload(systemError, correlationID)
			err := sender(systemErrorEventType, payload)
			if err != nil {
				logrus.WithFields(logrus.Fields{
					"correlation_id": correlationID,
				}).Errorf("Error sending webhook notification: %v", err)
			}
		}
	}(systemError)
}

// systemErrorEventType is the event name system.error is published under.
//
// A constant rather than a literal at the call site, because it is one of the thirteen
// event strings the event catalogue routes on: model.EventCategory maps it to the
// internal system category, and a typo here would route the event to the quarantine
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
// actionable half — it will not clear on its own. Timeout is checked before transport
// for the same reason in reverse: "i/o timeout" is a timeout that happens to be on a
// connection, and calling it a transport failure would send an operator looking at the
// wrong thing.
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
// error itself. That property is what makes this a sanitizer rather than a formatter of
// one: no error, however constructed, can produce output describing the deployment.
//
// A typed apierror is classified from its CODE first, because the code is an explicit
// classification the raising site already made and is strictly better than inferring one
// from prose. Text matching is the fallback for the many errors in this codebase that
// are plain fmt.Errorf values.
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
// It reads the code's HTTP status rather than enumerating every code, so a code added to
// apierror later is classified without an edit here. Enumerating them would mean this
// function silently returning "" for every new code — the failure mode where a growing
// vocabulary quietly degrades into unclassified.
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

// newCorrelationID returns the identifier that ties a system.error event to the log line
// carrying the full error.
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

// sanitizedSystemErrorPayload builds the system.error event body.
//
// # DATA-01: the raw error is not a payload
//
// This payload used to be {"error": systemError.Error(), "time": now}, and the error
// text is the problem. Blnk's internal errors describe the inside of the deployment:
// a PostgreSQL error renders with the schema, table, column, constraint, source file
// and routine that produced it; a Kafka or network error renders as
// "write tcp 10.0.0.4:34918->10.0.0.7:9092: broken pipe", naming internal addresses
// and broker topology; a validation error can quote the offending input, which in a
// ledger is somebody's account reference or amount. None of that is knowledge the
// recipient of an error notification needs, and all of it is durable once published —
// a Kafka topic is replicated and retained, and the event is also stored in
// blnk.event_outbox and copied to a dead-letter topic if it fails.
//
// system.error routes to the internal blnk.system category, which is excluded from
// model.SubscriberGrantableTopics, so no subscriber can be granted it. That is the first
// line of defence and it is not the only one needed: the topic is still readable by any
// principal with cluster-wide grants, the payload still lands in the outbox table that
// GET /events/dead-letter projects, and "no subscriber can read this topic today" is a
// property of the ACL model rather than of the payload.
//
// What is published instead is a diagnosis plus a handle:
//
//   - reason: one value from the fixed SystemErrorReason* vocabulary, which answers the
//     only question a recipient can act on — is this the database, a dependency, a
//     credential, a deadline, the input, or the configuration?
//   - error_code: the typed apierror code when the error carries one. Also a fixed
//     vocabulary, and the same value the HTTP API already returns for the condition.
//     Omitted rather than invented when there is none.
//   - correlation_id: the UUID logged alongside the full error, so an operator can
//     retrieve every detail from a channel whose audience is operators.
//   - time: unchanged from the original payload.
//
// Nothing is lost, only redirected. The complete error text is logged at the call site
// with this correlation ID attached.
//
// Parameters:
//   - systemError error: the error being notified. Read only for its classification and
//     its code; no part of its text reaches the result.
//   - correlationID string: the identifier already logged with the full error.
//
// Returns:
//   - map[string]interface{}: the event payload, containing no error text.
func sanitizedSystemErrorPayload(systemError error, correlationID string) map[string]interface{} {
	payload := map[string]interface{}{
		"reason":         classifySystemError(systemError),
		"correlation_id": correlationID,
		"time":           time.Now(),
	}

	// Omitted rather than empty when the error is untyped: an empty error_code key would
	// read as "the code is blank" instead of "there is no code".
	if code := systemErrorCode(systemError); code != "" {
		payload["error_code"] = code
	}

	return payload
}
