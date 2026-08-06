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
	"net/http"
	"strconv"
	"strings"
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
// into blnk.event_outbox inside the very database transaction that performed the
// ledger mutation, and the event relay in event_relay.go claims that row, publishes
// it to Kafka, and — for as long as the window is open — enqueues the legacy webhook
// task from THAT SAME CLAIMED ROW, through EnqueueLegacyWebhookDelivery. The relay's
// dual-delivery branch is therefore the one and only caller of that function.
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
// This file holds NO sunset logic, and that is intentional. The sunset is one
// decision with two consumers — the relay's dual-delivery branch and the HTTP 410
// Gone guard in api/middleware/sunset.go — and both ask WebhookSunsetPassed in
// event_sunset.go, which is the only place in this codebase that compares a clock
// against WEBHOOK_DEPRECATION_SUNSET_DATE. A date comparison added here would be a
// third, independently drifting copy of that decision. The functions below simply
// deliver whatever they are handed, whenever they are called; deciding whether they
// should be called at all belongs to the caller.
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
//   - Configured Notification.Webhook.Headers are applied last, so an operator can
//     add transport headers without the signing block being able to overwrite them.
//
// Retry is delegated, not owned. A non-2xx response returns an error so that asynq
// retries the delivery; swallowing it would permanently drop the webhook on a
// receiver-side failure. Exactly one HTTP attempt happens per call. The Kafka relay
// deliberately does NOT reuse this arrangement — a Kafka publish has no HTTP status
// to key on, so event_relay.go owns its own bounded exponential backoff. That
// difference lives entirely in the relay and changes nothing here.
//
// Parameters:
// - data NewWebhook: The webhook notification data to send.
// - client *http.Client: The HTTP client to use for the request.
//
// Returns:
// - error: An error if the request or processing fails.
func processHTTP(data NewWebhook, client *http.Client) error {
	payloadBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return processHTTPRaw(payloadBytes, client)
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
// body is the stored blnk.event_outbox payload, identical to the bytes published to Kafka.
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
// no secret is configured, configured headers applied last, and a non-2xx returned so asynq
// retries.
//
// Parameters:
//   - payloadBytes []byte: the body to send, verbatim. Never inspected, never rewritten.
//   - client *http.Client: the pooled client to send with.
//
// Returns:
//   - error: a configuration, request-construction, transport or non-2xx failure.
func processHTTPRaw(payloadBytes []byte, client *http.Client) error {
	conf, err := config.Fetch()
	if err != nil {
		return err
	}

	secret := conf.Server.SecretKey

	req, err := http.NewRequest(
		"POST",
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
		logrus.Warn("webhook sent unsigned: server.secret_key is not configured")
	}

	for key, value := range conf.Notification.Webhook.Headers {
		req.Header.Set(key, value)
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logrus.Warnf("Webhook failed with status %d", resp.StatusCode)
		// Returning the error lets asynq retry the delivery; swallowing it
		// would permanently drop the webhook on receiver-side failures.
		return fmt.Errorf("webhook delivery failed with status %d", resp.StatusCode)
	}

	return nil
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
// relay holds the authoritative bytes of the legacy body — the exact bytes recorded in
// blnk.event_outbox.payload inside the ledger transaction, and the exact bytes published to
// Kafka. Handing those to SendWebhook would mean decoding them into
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
// - _ context.Context: The context for the operation.
// - task *asynq.Task: The task containing the webhook notification data.
//
// Returns:
// - error: An error if the webhook processing fails.
func (b *Blnk) ProcessWebhook(_ context.Context, task *asynq.Task) error {
	conf, err := config.Fetch()
	if err != nil {
		return err
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
		logrus.Errorf("Error unmarshaling task payload: %v", err)
		return err
	}

	return processHTTPRaw(task.Payload(), b.httpClient)
}

// ===== SUNSET (terminal step of the Kafka event-streaming feature) =====
//
// This block is the executable contract for retiring the legacy HTTP transport. It is
// written as an ordered procedure because the order matters: step 1 before step 2
// preserves a contract that step 2 would otherwise destroy, and step 4 is a
// prohibition that step 3 makes tempting.
//
// PRECONDITION. Do none of this until WebhookSunsetPassed (event_sunset.go) answers
// true for the deployed WEBHOOK_DEPRECATION_SUNSET_DATE — that is, until the full
// 30-day dual-delivery window has elapsed. Until then this file must remain compiled
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
// Notification.Webhook configuration block. Kafka is then the sole transport, and the
// deprecated webhook management routes answer 410 Gone through
// api/middleware/sunset.go under the same WebhookSunsetPassed decision.
//
// STEP 6 — REMOVE NO DEPENDENCY. Nothing leaves go.mod at sunset. Deleting
// ProcessWebhook removes a handler registration, not a module: hibiken/asynq,
// hibiken/asynqmon and redis/go-redis all remain required by the transaction queue,
// the hooks subsystem and the index queue. Pruning them would break features that
// merely shared infrastructure with this one.
//
// ===== END SUNSET =====
