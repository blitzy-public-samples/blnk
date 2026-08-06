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
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/config"
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
// task from THAT SAME CLAIMED ROW. The relay's dual-delivery branch is therefore the
// one and only caller of SendWebhook.
//
// That single change of caller is the entire mechanism behind the dual-delivery
// payload-equivalence guarantee. Both transports read one row, so they cannot carry
// divergent bytes: equivalence is structural, not something the two code paths have
// to be kept in step by hand. Deleting this file before the window closes would
// remove the second transport that the guarantee is measured against.
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
// message to a topic just as they used to name a webhook. It is reused verbatim by
// the Kafka path rather than reimplemented, so that one status can never resolve to
// two different event names depending on which transport is looking at it. Like
// NewWebhook, it outlives this file: at sunset it moves, unmodified, to
// event_topics.go.
//
// DELIBERATELY PRESERVED DEFECT — do not "fix" this in passing. StatusCommit
// ("COMMIT", declared in transaction_inflight.go) has no case below, so it falls
// through to transaction.unknown. That is pre-existing behaviour, not a regression
// introduced by the Kafka work, and it is kept exactly as-is on purpose: the
// dual-delivery comparison asserts that the Kafka message and the legacy webhook
// carry identical bytes for the same event, and adding a transaction.commit case
// here would change one side of that comparison and fail it for a reason that has
// nothing to do with the transport. The behaviour is documented in
// docs/event-streaming.md so it can be corrected later as a deliberate, separately
// reviewed change — with the subscriber-facing event-name change that implies.
//
// Parameters:
// - status string: The status of the transaction.
//
// Returns:
// - string: The corresponding event string for the transaction status.
func getEventFromStatus(status string) string {
	switch strings.ToLower(status) {
	case strings.ToLower(StatusQueued):
		return "transaction.queued"
	case strings.ToLower(StatusApplied):
		return "transaction.applied"
	case strings.ToLower(StatusScheduled):
		return "transaction.scheduled"
	case strings.ToLower(StatusInflight):
		return "transaction.inflight"
	case strings.ToLower(StatusVoid):
		return "transaction.void"
	case strings.ToLower(StatusRejected):
		return "transaction.rejected"
	default:
		// StatusCommit lands here. See the note above and docs/event-streaming.md;
		// this fall-through is intentional and must not be given a case.
		return "transaction.unknown"
	}
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
	conf, err := config.Fetch()
	if err != nil {
		return err
	}

	payloadBytes, err := json.Marshal(data)
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

// SendWebhook enqueues a webhook notification task using the Blnk instance's asynq client.
//
// The only caller of this method is the dual-delivery branch of the event relay in
// event_relay.go, which invokes it with the payload of the same blnk.event_outbox row
// it has just published to Kafka. No domain post-action calls it any more. Adding a
// direct domain caller would reintroduce a second, independent source of webhook
// payloads and forfeit the equivalence guarantee described at the top of this file.
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
	var payload NewWebhook
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		logrus.Errorf("Error unmarshaling task payload: %v", err)
		return err
	}
	err = processHTTP(payload, b.httpClient)
	if err != nil {
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
// STEP 2 — DELETE. Remove processHTTP, SendWebhook and ProcessWebhook, then this file,
// then webhooks_test.go and webhooks_process_test.go. Those two test files cover the
// HTTP transport specifically and have no subject once it is gone; the payload and
// vocabulary behaviours they also touch are by then covered where those symbols now
// live.
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
