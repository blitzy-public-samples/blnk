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

// event_outbox.go is the producer-facing edge of the Kafka event-publishing
// pipeline. It does exactly two things, and deliberately nothing else:
//
//  1. PrepareEventOutbox turns a domain event into a blnk.event_outbox row. It is
//     modelled on PrepareLineageOutbox, which is the house pattern for outbox row
//     construction in this repository, and it is where THE PAYLOAD-PRESERVATION
//     GUARANTEE is implemented rather than merely asserted.
//  2. PublishEvent (and its in-transaction sibling PublishEventInTx) persists that
//     row. It is the one-for-one transport substitution for SendWebhook at every
//     producer call site.
//
// # The pipeline this file feeds
//
//	producer call site
//	    └─▶ PrepareEventOutbox            (this file: build the row)
//	          └─▶ InsertEventOutbox[InTx]  (database/event_outbox.go: status = pending)
//	                └─▶ EventRelayProcessor claims the row, publishes it to Kafka,
//	                    marks it dispatched — and, during the dual-delivery window,
//	                    enqueues the legacy webhook task FROM THE SAME ROW
//
// Two consequences of that shape are load-bearing and easy to get wrong:
//
//   - NOTHING HERE TALKS TO KAFKA. This file holds no Kafka import, no writer and
//     no broker address. The relay is the only caller of a kafka.Writer. A producer
//     that published inline would block a ledger write on broker availability and
//     would break the transactional-outbox guarantee it exists to provide.
//   - NOTHING HERE HOLDS SQL. Every statement lives in database/event_outbox.go and
//     is reached through the datasource interface. This file never opens a database
//     transaction either; when a caller already holds one, it passes it in.
//
// Retry, backoff, dead-lettering and replay are likewise absent by design — they
// belong to the relay and the dead-letter layer, which act on rows this file
// creates.

package blnk

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
)

// ---------------------------------------------------------------------------
// SUNSET RELOCATION NOTICE — NewWebhook moves INTO this file at sunset
// ---------------------------------------------------------------------------
//
// NewWebhook and getEventFromStatus are declared in webhooks.go today, and
// webhooks.go is deleted at the end of the 30-day dual-delivery window. Those two
// symbols must NOT go with it:
//
//   - NewWebhook IS the payload contract. Its marshaled two-key form,
//     {"event": ..., "data": ...}, is what PrepareEventOutbox stores in the outbox
//     payload column, so it outlives the legacy HTTP transport by definition.
//   - getEventFromStatus IS the transaction event-string vocabulary. It is the sole
//     producer of the seven transaction.* event names, including the
//     transaction.unknown that the COMMIT status falls through to.
//
// At sunset both declarations RELOCATE HERE, verbatim. Both files are in package
// blnk, so the move is textual: no import changes, no call-site changes, and no
// behavioural change whatsoever. Deleting webhooks.go without relocating them
// first would delete the payload contract along with the transport, which is the
// one thing the sunset must not do.
//
// They are deliberately NOT declared here now. Two declarations of the same
// identifier in one package do not compile, so webhooks.go remains their sole home
// for the duration of the window.
//
// ---------------------------------------------------------------------------

// defaultEventMaxAttempts is the retry budget stamped on an outbox row when the
// configured relay attempt limit is unavailable or nonsensical.
//
// It is 5 for three independent and agreeing reasons: it is the value
// PrepareLineageOutbox hardcodes on every lineage row, it is the default of
// RELAY_MAX_RETRY_ATTEMPTS (config.defaultRelay.MaxRetryAttempts), and it is the
// DEFAULT on the blnk.event_outbox.max_attempts column. A row therefore ends up
// with the same budget whether it is set here, defaulted by configuration, or
// defaulted by the database.
const defaultEventMaxAttempts = 5

// unkeyedEventPartitionKey is the terminal fallback for the Kafka message key.
//
// The key decides the partition, and the partition decides ordering: every event
// sharing a key is appended to one partition and is therefore consumed in publish
// order. An EMPTY key is the failure mode this constant exists to prevent — Kafka
// treats a null/empty key as "any partition", so unkeyed events are scattered
// round-robin and their relative order is lost silently, with nothing in the data
// to show that it happened.
//
// This value is reached only when an event has no aggregate identifier AND no
// event type to fall back to, which in practice means a caller passed a
// zero-valued NewWebhook. Routing those to one fixed key keeps them ordered
// amongst themselves and keeps them visible, and the distinctive value makes them
// trivial to spot in the outbox: `WHERE ledger_id = 'blnk.unkeyed'`.
const unkeyedEventPartitionKey = "blnk.unkeyed"

// eventConfiguration reads the configuration this file needs, tolerating both an
// uninitialised Blnk instance and an unloaded configuration store.
//
// The non-nil path goes through (*Blnk).Config(), which is how SendWebhook reads
// configuration today: it prefers the instance's cached configuration and falls
// back to the store, returning an empty Configuration if even that fails. An empty
// Configuration reads as "event publishing is not configured", which is exactly
// the degradation this file wants — see eventPublishingConfigured.
//
// The nil-receiver path exists because these entry points are documentation of a
// contract as much as they are code: a nil *Blnk must produce a no-op, not a
// panic. It reads through the package's fetchConfiguration seam (declared in
// event_sunset.go) rather than config.Fetch directly, so that a test which swaps
// that seam sees consistent behaviour across the event files.
//
// Returns:
//   - *config.Configuration: the effective configuration, or nil when none can be
//     resolved. Callers must treat nil as "not configured".
func (l *Blnk) eventConfiguration() *config.Configuration {
	if l != nil {
		return l.Config()
	}

	cnf, err := fetchConfiguration()
	if err != nil {
		return nil
	}

	return cnf
}

// eventPublishingConfigured reports whether this deployment has asked for events
// at all.
//
// THE NO-OP-WHEN-UNCONFIGURED CONTRACT LIVES HERE. SendWebhook returns nil the
// moment it sees an empty webhook URL, which is why Blnk runs perfectly well with
// no notification sink at all, and why the existing test suite — including the
// NewBlnk(nil) construction in webhooks_test.go — works without a broker or an
// HTTP endpoint anywhere in sight. Reproducing that contract is not a
// convenience; a publisher that errored or blocked when unconfigured would break
// every such deployment and every such test.
//
// Either transport being configured is enough to justify capturing the event,
// because the outbox row feeds BOTH of them: during the dual-delivery window the
// relay publishes the row to Kafka and enqueues the legacy webhook task from that
// same row. A deployment mid-migration with only a webhook URL set still needs its
// rows captured, and a deployment past the sunset with only brokers set obviously
// does too.
//
// Brokers are checked for a non-blank entry rather than merely for a non-empty
// slice. KAFKA_BROKERS is parsed by envconfig as a comma-separated list, so
// KAFKA_BROKERS="" and KAFKA_BROKERS="," both yield a slice that is non-empty but
// carries nothing usable; treating those as configured would capture rows that no
// relay could ever publish.
//
// Parameters:
//   - cnf *config.Configuration: the configuration to inspect. May be nil.
//
// Returns:
//   - bool: true when Kafka brokers or a legacy webhook URL are configured.
func eventPublishingConfigured(cnf *config.Configuration) bool {
	if cnf == nil {
		return false
	}

	for _, broker := range cnf.Kafka.Brokers {
		if strings.TrimSpace(broker) != "" {
			return true
		}
	}

	return strings.TrimSpace(cnf.Notification.Webhook.Url) != ""
}

// eventMaxAttempts resolves the per-row retry budget from RELAY_MAX_RETRY_ATTEMPTS.
//
// The budget is stamped on the row rather than read by the relay at publish time so
// that an operator can extend the budget for one stuck event without reconfiguring
// and restarting the relay, and so that a configuration change never retroactively
// alters the budget of rows already in flight.
//
// A non-positive configured value is replaced with the default instead of being
// honoured. Zero would mean "never attempt", which would strand the row in the
// pending state forever: the relay would have no attempts to spend, and the row
// would neither publish nor dead-letter. config.setRelayDefaults already warns on
// such a value; this is the second half of that defence, at the point where the
// value would otherwise become durable.
//
// Parameters:
//   - cnf *config.Configuration: the configuration to read. May be nil.
//
// Returns:
//   - int: a strictly positive retry budget.
func eventMaxAttempts(cnf *config.Configuration) int {
	if cnf != nil && cnf.Relay.MaxRetryAttempts > 0 {
		return cnf.Relay.MaxRetryAttempts
	}

	return defaultEventMaxAttempts
}

// mapStringValue extracts a trimmed string value from a map payload, tolerating a
// value that is not a string.
//
// Bulk transaction events carry a map[string]interface{} rather than a typed
// struct, so their identifiers have to be read dynamically. A key that is absent,
// nil, blank, or of some other type yields the empty string, which sends the caller
// on to the next step of its fallback chain rather than producing a nonsense key
// such as "<nil>" or "%!s(int=7)".
//
// Parameters:
//   - payload map[string]interface{}: the map to read. May be nil.
//   - key string: the key to read.
//
// Returns:
//   - string: the trimmed string value, or "" when it is missing or not a string.
func mapStringValue(payload map[string]interface{}, key string) string {
	raw, ok := payload[key]
	if !ok {
		return ""
	}

	value, ok := raw.(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(value)
}

// eventAggregateID derives the aggregate identifier — the entity the event is
// ABOUT — from the payload object the producer handed over.
//
// This is the value a consumer groups by once messages arrive. It is deliberately
// distinct from the Kafka message key returned by eventLedgerID: the key controls
// partitioning and therefore ordering, whereas this identifies the subject of the
// event. The two coincide for some event types and differ for others.
//
// Derivation, covering every one of the thirteen event strings Blnk emits:
//
//	ledger.created            *model.Ledger         → LedgerID
//	identity.created          *model.Identity       → IdentityID
//	balance.created           *model.Balance        → BalanceID
//	balance.monitor            model.BalanceMonitor → MonitorID (the monitor that fired)
//	transaction.queued         *model.Transaction   → TransactionID
//	transaction.applied        *model.Transaction   → TransactionID
//	transaction.scheduled      *model.Transaction   → TransactionID
//	transaction.inflight       *model.Transaction   → TransactionID
//	transaction.void           *model.Transaction   → TransactionID
//	transaction.rejected       model.Transaction    → TransactionID
//	transaction.unknown        *model.Transaction   → TransactionID
//	bulk_transaction.<status>  map[string]any       → batch_id
//	system.error               map[string]any       → "" (no aggregate; the caller falls back)
//
// BOTH THE POINTER AND THE VALUE FORM of each struct are matched, and that is not
// defensive padding. The producer call sites genuinely disagree: transaction
// execution passes *model.Transaction while the worker's rejection handler passes
// model.Transaction by value, and the balance monitor check passes
// model.BalanceMonitor by value. A type switch covering only pointers would
// silently fall through to the empty string for two real event types.
//
// A nil pointer of a matched type yields the empty string rather than panicking.
// Its JSON payload marshals to "null", so there is nothing to derive from and the
// caller's fallback chain is the right answer.
//
// Parameters:
//   - payload interface{}: the NewWebhook payload object. May be nil or of any type.
//
// Returns:
//   - string: the aggregate identifier, or "" when the payload carries none.
func eventAggregateID(payload interface{}) string {
	switch typed := payload.(type) {
	case *model.Transaction:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.TransactionID)
	case model.Transaction:
		return strings.TrimSpace(typed.TransactionID)
	case *model.Balance:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.BalanceID)
	case model.Balance:
		return strings.TrimSpace(typed.BalanceID)
	case *model.BalanceMonitor:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.MonitorID)
	case model.BalanceMonitor:
		return strings.TrimSpace(typed.MonitorID)
	case *model.Ledger:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.LedgerID)
	case model.Ledger:
		return strings.TrimSpace(typed.LedgerID)
	case *model.Identity:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.IdentityID)
	case model.Identity:
		return strings.TrimSpace(typed.IdentityID)
	case map[string]interface{}:
		// Bulk transaction events. batch_id is the aggregate: one batch produces a
		// sequence of bulk_transaction.<status> events describing its progress, and
		// grouping them by batch is what makes that sequence readable.
		return mapStringValue(typed, "batch_id")
	default:
		// Any other payload shape, system.error's {"error", "time"} map included
		// (it is a map[string]interface{} with no batch_id, so it arrives here via
		// the map arm returning ""). There is no aggregate to name; the caller
		// falls back.
		return ""
	}
}

// eventLedgerID derives the KAFKA MESSAGE KEY for an event.
//
// This value is the single most consequential field this file computes. The key is
// hashed by a stable balancer to select a partition, so every event sharing a key
// lands on one partition and is consumed in publish order, while events with
// different keys carry no ordering relationship at all. Per-aggregate ordering is
// therefore not a property of the broker or of the relay — it is a property of THIS
// FUNCTION returning a stable value for every event belonging to one aggregate.
//
// Derivation, and the reasoning behind each choice:
//
//	*model.Ledger / model.Ledger
//	    LedgerID. The event is about the ledger itself, so the ledger is the key.
//
//	*model.Balance / model.Balance
//	    LedgerID, falling back to BalanceID. A balance belongs to exactly one
//	    ledger, so keying by the ledger co-locates every balance event of that
//	    ledger on one partition and orders them against each other. The fallback
//	    covers a balance whose ledger is not populated on the payload.
//
//	model.BalanceMonitor / *model.BalanceMonitor
//	    BalanceID, falling back to MonitorID. BalanceMonitor carries no ledger
//	    field, and the balance it watches is the closest stable aggregate: every
//	    alert for one balance stays ordered, which is what an alert consumer needs.
//
//	*model.Identity / model.Identity
//	    IdentityID. Identity carries no ledger field either — an identity is not
//	    scoped to a ledger in this model — so the identity is its own aggregate.
//
//	*model.Transaction / model.Transaction
//	    Source, falling back to Destination, then TransactionID.
//	    model.Transaction HAS NO LEDGER FIELD; its ledger association is indirect,
//	    through the balances it moves value between. The source balance is used
//	    because it is exactly what the existing transaction queue shards on —
//	    hashBalanceID(transaction.Source) in queue.go — so Kafka partitioning and
//	    queue sharding agree, and the ordering guarantee subscribers observe
//	    matches the ordering the ledger itself already imposes on that balance.
//	    Source is also stable across a transaction's whole lifecycle, so
//	    transaction.queued, .inflight and .applied for one transaction share a
//	    partition and can never be observed out of order. Destination covers a
//	    credit-only transaction; TransactionID is the last resort for a multi-source
//	    parent whose own Source and Destination are unset.
//
//	map[string]interface{}
//	    batch_id. A bulk batch is the natural aggregate, so a batch's progress
//	    events stay ordered relative to one another.
//
// system.error and anything else reach the caller's fallback chain, which is
// documented at PrepareEventOutbox. NOTHING here returns a key that would leave the
// partition assignment to chance.
//
// Parameters:
//   - payload interface{}: the NewWebhook payload object. May be nil or of any type.
//
// Returns:
//   - string: the partition key, or "" when the payload carries none.
func eventLedgerID(payload interface{}) string {
	switch typed := payload.(type) {
	case *model.Transaction:
		if typed == nil {
			return ""
		}
		return transactionPartitionKey(typed.Source, typed.Destination, typed.TransactionID)
	case model.Transaction:
		return transactionPartitionKey(typed.Source, typed.Destination, typed.TransactionID)
	case *model.Balance:
		if typed == nil {
			return ""
		}
		return firstNonBlank(typed.LedgerID, typed.BalanceID)
	case model.Balance:
		return firstNonBlank(typed.LedgerID, typed.BalanceID)
	case *model.BalanceMonitor:
		if typed == nil {
			return ""
		}
		return firstNonBlank(typed.BalanceID, typed.MonitorID)
	case model.BalanceMonitor:
		return firstNonBlank(typed.BalanceID, typed.MonitorID)
	case *model.Ledger:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.LedgerID)
	case model.Ledger:
		return strings.TrimSpace(typed.LedgerID)
	case *model.Identity:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.IdentityID)
	case model.Identity:
		return strings.TrimSpace(typed.IdentityID)
	case map[string]interface{}:
		return mapStringValue(typed, "batch_id")
	default:
		return ""
	}
}

// transactionPartitionKey applies the transaction key preference documented on
// eventLedgerID: source balance, then destination balance, then the transaction
// itself.
//
// Extracted so the pointer and value arms of the type switch cannot drift apart —
// two copies of a three-step preference is two chances to reorder one of them, and
// a reordering here would silently change which partition a transaction's events
// land on.
//
// Parameters:
//   - source, destination, transactionID string: the candidate keys, in preference
//     order.
//
// Returns:
//   - string: the first non-blank candidate, or "" when all three are blank.
func transactionPartitionKey(source, destination, transactionID string) string {
	return firstNonBlank(source, destination, transactionID)
}

// firstNonBlank returns the first candidate that is not empty once trimmed, or ""
// when every candidate is blank.
//
// Trimming matters because a whitespace-only identifier is indistinguishable from a
// missing one for keying purposes, yet " " and "" hash to different partitions.
// Normalising here means a stray space in an identifier cannot silently split one
// aggregate's events across two partitions.
//
// Parameters:
//   - candidates ...string: the candidates, in preference order.
//
// Returns:
//   - string: the first non-blank candidate, trimmed, or "".
func firstNonBlank(candidates ...string) string {
	for _, candidate := range candidates {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			return trimmed
		}
	}

	return ""
}

// PrepareEventOutbox builds the blnk.event_outbox row for a domain event, ready to
// be inserted either standalone or inside a caller's ledger transaction.
//
// It is modelled directly on PrepareLineageOutbox and behaves the same way in every
// respect a reviewer would compare: it opens a span, returns nil when there is no
// work to do, marshals the payload once, and — critically — treats a marshal failure
// as a logged non-event rather than an error to propagate.
//
// # THE PAYLOAD-PRESERVATION GUARANTEE
//
// The single most important line in this file is the json.Marshal below. It
// marshals THE WHOLE NewWebhook VALUE — both keys — which is byte-for-byte the
// same operation SendWebhook performs to build today's HTTP webhook body:
//
//	{"event": "transaction.applied", "data": { ...the payload object... }}
//
// The entire two-key object is carried, not just the inner "data". That is the
// resolved reading of "payload matching today's webhook body field-for-field", and
// it is the strongest one available: an existing subscriber's body parser keeps
// working unchanged and only the transport differs. Carrying only the inner object
// would force every subscriber to change their parser, which is the opposite of
// preserving the payload.
//
// Because the bytes are produced here once and every downstream reader takes them
// from the stored row, two guarantees become structural rather than a matter of
// careful coding: during the dual-delivery window the Kafka message and the legacy
// webhook body are identical (both are read from this one row), and a dead-letter
// replay matches the original because it re-publishes the stored bytes instead of
// re-marshalling a struct.
//
// The event name is additionally hoisted to the envelope's EventType field, so it
// appears both there and inside the payload. That redundancy is deliberate: it lets
// subscribers, the relay and SQL-side triage route and filter without parsing the
// payload at all, at a cost of one short string per message.
//
// # Return-nil cases, and why neither is an error
//
//   - Event publishing is not configured. Reproducing SendWebhook's
//     no-op-when-unconfigured contract is what lets Blnk run with no notification
//     sink; see eventPublishingConfigured.
//   - The payload cannot be marshaled — a channel, a function, or a cyclic
//     structure somewhere inside it. This mirrors PrepareLineageOutbox exactly: log
//     it, record it on the span, and return nil. A MALFORMED PAYLOAD MUST NEVER
//     TAKE DOWN A LEDGER WRITE. Propagating an error here would abort the enclosing
//     ledger transaction and reject a financially valid mutation because of a
//     notification defect, which is the wrong trade in a ledger by a wide margin.
//
// A nil return is safe for every consumer: the atomic writers' variadic event
// parameter skips nil entries, and publishEvent treats nil as "nothing to persist".
//
// # Use with the atomic writers (requirement R-2)
//
// This is the in-transaction entry point. Callers that are about to perform a
// ledger mutation prepare the row here and thread it through the atomic writers'
// variadic event-outbox parameter, exactly as PrepareLineageOutbox's result is
// threaded through today:
//
//	eventRow := l.PrepareEventOutbox(ctx, NewWebhook{Event: eventName, Payload: txn})
//	txn, err := l.datasource.RecordTransactionWithBalancesAndOutbox(
//	        ctx, txn, sourceBalance, destinationBalance, lineageRow, eventRow)
//
// The row is then inserted inside the very same database transaction as the balance
// updates, so the mutation and its event commit or roll back together. Callers with
// no ledger transaction to enrol in use PublishEvent instead.
//
// Parameters:
//   - ctx context.Context: the context for the operation, used for tracing only.
//   - event NewWebhook: the event name and payload object, passed through unchanged
//     from the producer call site.
//
// Returns:
//   - *model.EventOutbox: the row to persist, or nil when publishing is
//     unconfigured or the payload cannot be marshaled.
func (l *Blnk) PrepareEventOutbox(ctx context.Context, event NewWebhook) *model.EventOutbox {
	_, span := tracer.Start(ctx, "PrepareEventOutbox")
	defer span.End()

	cnf := l.eventConfiguration()
	if !eventPublishingConfigured(cnf) {
		span.AddEvent("Event publishing not configured")
		return nil
	}

	// THE PAYLOAD GUARANTEE: marshal the whole NewWebhook value, both keys, exactly
	// as SendWebhook does. Never the inner payload alone, never re-shaped, never
	// re-keyed.
	payloadBytes, err := json.Marshal(event)
	if err != nil {
		logrus.Errorf("failed to marshal event outbox payload for event %q: %v", event.Event, err)
		span.RecordError(err)
		return nil
	}

	eventType := strings.TrimSpace(event.Event)
	aggregateID := eventAggregateID(event.Payload)
	ledgerID := eventLedgerID(event.Payload)

	// The documented fallback chain. Its purpose is a partition key that is always
	// present and always deterministic: an empty key would let Kafka scatter the
	// event round-robin and destroy ordering with nothing in the data to show it.
	//
	// Falling back to the event type is what gives system.error — which has no
	// aggregate of any kind — a single partition and therefore a total order, which
	// is precisely what an error stream wants. The sentinel is reached only when
	// there is no event type either.
	if ledgerID == "" {
		ledgerID = aggregateID
	}
	if ledgerID == "" {
		ledgerID = eventType
	}
	if ledgerID == "" {
		ledgerID = unkeyedEventPartitionKey
	}
	// aggregate_id is NOT NULL in the schema and is what consumers group by, so it
	// inherits the key once every payload-derived candidate is exhausted.
	if aggregateID == "" {
		aggregateID = ledgerID
	}

	outbox := &model.EventOutbox{
		EventID:     uuid.New().String(),
		EventType:   eventType,
		AggregateID: aggregateID,
		LedgerID:    ledgerID,
		// Resolved once, at construction, and stored on the row. The relay never
		// re-derives it, so a row stays replayable to its ORIGINAL destination even
		// if KAFKA_TOPIC_PREFIX changes afterwards.
		Topic:         TopicForEvent(eventType),
		SchemaVersion: model.SchemaVersionV1,
		Payload:       payloadBytes,
		// UTC so the RFC3339 rendering on the wire is unambiguous and identical
		// wherever the process runs. time.Time's standard JSON encoding is already
		// RFC3339, so no custom marshaller is involved.
		OccurredAt: time.Now().UTC(),
		// Set explicitly even though both the repository layer and the column
		// default would supply it, so the in-memory row the caller holds agrees
		// with the row that lands in the table.
		Status:      model.EventOutboxStatusPending,
		MaxAttempts: eventMaxAttempts(cnf),
	}

	span.AddEvent("Event outbox entry prepared", trace.WithAttributes(
		attribute.String("event.id", outbox.EventID),
		attribute.String("event.type", outbox.EventType),
		attribute.String("event.aggregate_id", outbox.AggregateID),
		attribute.String("event.ledger_id", outbox.LedgerID),
		attribute.String("event.topic", outbox.Topic),
	))

	return outbox
}

// PublishEvent captures a domain event in the transactional outbox. It is the
// one-for-one replacement for SendWebhook at every producer call site.
//
// The parameter stays a NewWebhook on purpose. Every call site changes by exactly
// one identifier — SendWebhook becomes PublishEvent — and the event string and
// payload object it passes are provably unchanged, which is what makes the
// payload-preservation guarantee verifiable by reading the diff rather than by
// trusting a description of it. The eight sites are the RegisterWebhookSender
// closure in blnk.go, the post-action hooks in ledger.go, identity.go and balance.go
// (twice), transaction_execution.go, transaction_bulk.go, and the worker's rejection
// handler in cmd/workers.go.
//
// THIS FUNCTION DOES NOT TALK TO KAFKA. It writes one pending row and returns. The
// relay claims that row, publishes it with bounded retry, dead-letters it if the
// retry budget is spent, and — during the dual-delivery window — enqueues the legacy
// webhook task from the same row. Keeping the broker off the producer's path is what
// makes the outbox worth having: a ledger write never waits on, and never fails
// because of, a broker.
//
// Error handling matches SendWebhook's so that call sites need no adjustment. A
// persistence failure is logged with structured context and returned, and the
// existing call sites route it to notification.NotifyError exactly as they do today.
// The no-op cases enumerated on publishEvent all return nil instead, because none of
// them is a failure of the mutation the caller just performed.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
//   - event NewWebhook: the event name and payload object, unchanged from the
//     producer call site.
//
// Returns:
//   - error: nil on success and on every no-op; the persistence error otherwise.
func (l *Blnk) PublishEvent(ctx context.Context, event NewWebhook) error {
	return l.publishEvent(ctx, nil, event)
}

// PublishEventInTx captures a domain event inside an existing database transaction,
// so the event and the ledger mutation that produced it commit or roll back
// together.
//
// This is the transactional-outbox guarantee (requirement R-2) at its narrowest: the
// caller has already begun a transaction and applied its mutation, and passing that
// same *sql.Tx here ties the event's fate to it. There is no window in which a
// balance moved but its event was lost, and none in which an event describes a
// mutation that was rolled back. That is what makes exactly-once semantics on the
// write side possible, layered over Kafka's at-least-once delivery.
//
// NO TRANSACTION IS OPENED HERE. The transaction is always the caller's, because
// only the caller knows what else belongs inside it; opening one here would produce
// a second, independent transaction and defeat the entire point. Callers that have
// no transaction to offer use PublishEvent, and a nil tx is accepted and routed to
// the standalone path so that a caller with a conditionally-open transaction needs
// no branch of its own.
//
// Note that the atomic writers in database/transaction.go do not call this method:
// they take their event rows as a variadic parameter and insert them within the
// transaction they own. Prepare those rows with PrepareEventOutbox. This method
// serves any caller that holds a *sql.Tx directly.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
//   - tx *sql.Tx: the caller's open transaction. May be nil, which selects the
//     standalone insert.
//   - event NewWebhook: the event name and payload object, unchanged from the
//     producer call site.
//
// Returns:
//   - error: nil on success and on every no-op; the persistence error otherwise.
func (l *Blnk) PublishEventInTx(ctx context.Context, tx *sql.Tx, event NewWebhook) error {
	return l.publishEvent(ctx, tx, event)
}

// publishEvent is the single implementation behind PublishEvent and
// PublishEventInTx.
//
// Both entry points share one body so the two paths cannot diverge: they build the
// row identically and differ only in which repository method persists it. A second
// copy of the guard sequence would be a second place for the no-op contract to rot.
//
// The three no-op cases, each returning nil:
//
//   - PrepareEventOutbox returned nil, meaning publishing is not configured or the
//     payload would not marshal. Both are already logged or traced at their source.
//   - The receiver is nil. A nil *Blnk must not panic here; these methods are called
//     from goroutines spawned by post-action hooks, where a panic would take down
//     the process rather than surface as an error.
//   - The datasource is nil. NewBlnk(nil) is a real, supported construction — the
//     existing webhook tests use it — so there is genuinely nowhere to persist to.
//     It is logged at warning level rather than silently ignored, because in a
//     deployment that DOES have publishing configured a nil datasource means events
//     are being dropped and an operator needs to know.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
//   - tx *sql.Tx: the caller's transaction, or nil for the standalone insert.
//   - event NewWebhook: the event to capture.
//
// Returns:
//   - error: nil on success and on every no-op; the persistence error otherwise.
func (l *Blnk) publishEvent(ctx context.Context, tx *sql.Tx, event NewWebhook) error {
	ctx, span := tracer.Start(ctx, "PublishEvent")
	defer span.End()

	outbox := l.PrepareEventOutbox(ctx, event)
	if outbox == nil {
		span.AddEvent("No event captured")
		return nil
	}

	span.SetAttributes(
		attribute.String("event.id", outbox.EventID),
		attribute.String("event.type", outbox.EventType),
		attribute.String("event.topic", outbox.Topic),
		attribute.Bool("event.in_transaction", tx != nil),
	)

	// Checked AFTER the row is built, deliberately. Preparing the row costs one
	// marshal and no I/O, and it is what gives this warning the event identity that
	// makes it actionable — "some event was dropped" is not a diagnosable message.
	// The nil-receiver arm is evaluated first so the field access can never run on a
	// nil pointer.
	if l == nil || l.datasource == nil {
		logrus.WithFields(logrus.Fields{
			"event_id":   outbox.EventID,
			"event_type": outbox.EventType,
			"topic":      outbox.Topic,
		}).Warn("event not captured: no datasource is configured on this Blnk instance")
		span.AddEvent("Event dropped: no datasource")

		return nil
	}

	var err error
	if tx != nil {
		// Inside the caller's ledger transaction: the event commits with the
		// mutation or not at all.
		err = l.datasource.InsertEventOutboxInTx(ctx, tx, outbox)
	} else {
		// No accompanying ledger mutation — ledger.created, identity.created,
		// balance.created, balance.monitor, bulk_transaction.<status> and
		// system.error all arrive here. They still belong in the outbox so they get
		// the same durable retry, dead-letter and replay treatment as every other
		// event; there is simply no wider transaction to enrol them in.
		err = l.datasource.InsertEventOutbox(ctx, outbox)
	}
	if err != nil {
		// Logged here for immediate operator visibility with the full event
		// identity, and returned so the caller's existing error handling — which
		// routes to notification.NotifyError at most call sites — behaves exactly as
		// it did with SendWebhook.
		logrus.WithFields(logrus.Fields{
			"event_id":       outbox.EventID,
			"event_type":     outbox.EventType,
			"topic":          outbox.Topic,
			"aggregate_id":   outbox.AggregateID,
			"in_transaction": tx != nil,
		}).WithError(err).Error("failed to record event in the outbox")
		span.RecordError(err)

		return err
	}

	span.AddEvent("Event recorded in outbox", trace.WithAttributes(
		attribute.Int64("event.outbox_id", outbox.ID),
	))

	return nil
}
