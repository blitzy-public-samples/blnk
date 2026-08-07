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
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
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

// eventPublishingConfigured reports whether this deployment has asked for events to be
// CAPTURED IN THE OUTBOX at all, which is to say whether Kafka is configured.
//
// THE NO-OP-WHEN-UNCONFIGURED CONTRACT LIVES HERE. SendWebhook returns nil the
// moment it sees an empty webhook URL, which is why Blnk runs perfectly well with
// no notification sink at all, and why the existing test suite — including the
// NewBlnk(nil) construction in webhooks_test.go — works without a broker or an
// HTTP endpoint anywhere in sight. Reproducing that contract is not a
// convenience; a publisher that errored or blocked when unconfigured would break
// every such deployment and every such test.
//
// # THE OUTBOX IS ONLY A DESTINATION WHEN A RELAY CAN DRAIN IT
//
// This used to answer true when EITHER transport was configured, on the reasoning that
// the row feeds both. That reasoning is only sound while a relay is running, and the
// relay REFUSES TO RUN without Kafka: it would otherwise be handed the no-op publisher,
// report every publish as dispatched and retire the whole outbox having sent nothing.
//
// So a webhook-only deployment — a webhook URL, no brokers, which is every deployment
// that has not started migrating — captured rows that nothing would ever claim, and
// because the producers no longer call SendWebhook themselves, its notifications simply
// stopped. Silently: no error, no failed delivery, a growing table.
//
// Capture is therefore tied to KAFKA being configured, and the legacy-only case is
// served by the transport it was always served by. publishEvent routes an event to
// SendWebhook directly when there are no brokers and a webhook URL is set, which is
// byte-for-byte the pre-migration behaviour. Once brokers ARE configured the outbox
// takes over both legs and the relay's dual-delivery branch is the only caller of the
// legacy transport, which is what keeps the two bodies identical.
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
//   - bool: true when at least one usable Kafka broker is configured.
func eventPublishingConfigured(cnf *config.Configuration) bool {
	if cnf == nil {
		return false
	}

	for _, broker := range cnf.Kafka.Brokers {
		if strings.TrimSpace(broker) != "" {
			return true
		}
	}

	return false
}

// legacyWebhookOnly reports that this deployment has a webhook URL and no Kafka broker,
// which is the pre-migration steady state and the state every deployment is in before it
// opts in.
//
// It is the guard on publishEvent's direct legacy delivery. Kafka being configured is
// checked FIRST by the caller, so this cannot divert an event away from the outbox on a
// deployment that has a relay: the outbox always wins when it can be drained.
//
// Parameters:
//   - cnf *config.Configuration: the configuration to inspect. May be nil.
//
// Returns:
//   - bool: true when a legacy webhook URL is configured.
func legacyWebhookOnly(cnf *config.Configuration) bool {
	if cnf == nil {
		return false
	}

	return strings.TrimSpace(cnf.Notification.Webhook.Url) != ""
}

// eventCaptureEnabled reports whether this process captures events at all.
//
// It exists so a caller can decide NOT TO ASK for atomic capture, rather than asking and
// receiving nothing. The three creation writers open a database transaction as soon as they are
// handed an event preparer — that is how the entity and its event commit together — so a
// deployment with no transport configured would otherwise pay a transaction per ledger,
// identity and balance creation in order to insert no event at all, and every existing test
// that asserts the exact statement those writers issue would see a BEGIN it did not expect.
//
// Returning false makes the preparer constructors hand back a nil preparer, which
// database.firstEventPreparer skips, which leaves the writer on its original
// single-statement path. That is the no-op-when-unconfigured contract this pipeline inherited
// from SendWebhook, applied one layer earlier than PrepareEventOutbox applies it.
//
// Returns:
//   - bool: true when Kafka brokers or a legacy webhook URL are configured.
func (l *Blnk) eventCaptureEnabled() bool {
	if l == nil {
		return false
	}

	return eventPublishingConfigured(l.eventConfiguration())
}

// publishEntityEventWhenUncaptured delivers a ledger, identity or balance creation event
// from the post-commit path WHEN, AND ONLY WHEN, the repository did not capture it.
//
// # WHY THIS FALLBACK EXISTS
//
// The three creation events are captured by a preparer that the repository invokes inside
// the mutation's own transaction, which is how requirement R-2 is met for them. The
// preparer constructors return nil when eventCaptureEnabled is false, so that a deployment
// with no broker does not pay a transaction per creation to insert no event.
//
// Those two facts together left a hole on the ONE deployment shape that has not opted in
// yet. A webhook-only deployment — a webhook URL, no KAFKA_BROKERS, which is every
// deployment before it migrates — has no preparer, so nothing is captured; and the
// post-commit publish that used to serve it was removed when capture moved into the
// repository. ledger.created, identity.created and balance.created were consequently
// delivered by NEITHER transport: no error, no failed delivery, simply gone. Every other
// producer in the package kept its publish call and therefore kept working, which is why
// this shows up only for these three.
//
// So the publish is restored, as a FALLBACK rather than as the primary path. It is the same
// arrangement postTransactionActions already uses for the seven status-derived events: the
// atomic writer is authoritative, and the post-commit call runs only for an event that
// writer did not record.
//
// # WHY IT CANNOT DOUBLE-PUBLISH
//
// The guard is the same predicate the preparer constructors branch on, so the two are
// mutually exclusive by construction: whenever a preparer was supplied this returns before
// doing anything, and whenever it was not there is no captured row to conflict with. That
// matters beyond tidiness — event_id is derived from the aggregate's identity, so a second
// capture would be refused by the unique index and every creation would log a conflict.
//
// PublishEvent then applies its own gate: with no brokers and a webhook URL it goes
// straight down the legacy transport, and with neither configured it is a no-op. This
// function therefore has no effect at all on a deployment that has no notification sink,
// which is the contract inherited from SendWebhook.
//
// # WHY THE AGGREGATE ID IS CHECKED
//
// CreateBalance reports a unique_indicator_currency violation as success with an EMPTY
// balance — its long-standing idempotent-create contract — and the repository declines to
// capture on that path precisely because balance.created would otherwise announce a
// creation that did not happen, with an empty payload. Restoring a publish without this
// guard would restore that defect along with it. An empty aggregate id means nothing was
// created, so nothing is announced.
//
// Parameters:
//   - ctx context.Context: the publishing context. Callers detach it from request
//     cancellation before handing it over; see postBalanceActions.
//   - aggregateID string: the created entity's identifier. Empty means nothing was created.
//   - event NewWebhook: the event string and payload object, unchanged from what the legacy
//     transport was handed.
//   - options ...EventOption: applied only on the outbox path, which this fallback cannot
//     reach; passed so the two capture sites read identically.
//
// Returns:
//   - error: SendWebhook's enqueue error, exactly as the pre-migration call site returned
//     it, or nil when there was nothing to do.
func (l *Blnk) publishEntityEventWhenUncaptured(ctx context.Context, aggregateID string, event NewWebhook, options ...EventOption) error {
	if l == nil {
		return nil
	}

	// The repository already captured this event inside the mutation's transaction.
	if l.eventCaptureEnabled() {
		return nil
	}

	// Nothing was created, so there is no creation to announce.
	if strings.TrimSpace(aggregateID) == "" {
		return nil
	}

	return l.PublishEvent(ctx, event, options...)
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
// distinct from the Kafka message key returned by eventPartitionKey: the key controls
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

// eventPartitionKey derives the KAFKA MESSAGE KEY for an event.
//
// # IT IS NOT THE LEDGER ID, and it used to be called one
//
// This function was named eventLedgerID and its result was stored in a column called
// ledger_id, which was wrong for most of the values it actually produced: depending
// on the event it returns a ledger id, a source or destination BALANCE id, an
// IDENTITY id, a MONITOR id, a BATCH id, or the event type itself. Two things
// followed from the misnaming, and both were real rather than cosmetic. Anything
// reading ledger_id to learn which ledger an event belonged to got a balance id for
// every transaction event and had no way to tell. And any subscriber-facing claim
// that a message-key prefix identifies a ledger was simply unfounded, because for
// the highest-volume event type in the system the key is not a ledger id at all.
//
// The authoritative ledger now lives in its own column, populated by eventLedgerID,
// which returns a value ONLY when the payload genuinely carries one. The two
// functions answer different questions and must not be conflated again:
// eventPartitionKey answers "where does this message go", eventLedgerID answers
// "which ledger is this about".
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
func eventPartitionKey(payload interface{}) string {
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

// eventLedgerID resolves the AUTHORITATIVE ledger an event belongs to, or "" when the
// event genuinely has no ledger.
//
// It takes no part in partitioning, in ordering, or in routing. Its only job is to
// answer "which ledger is this about" honestly, which means returning nothing rather
// than something plausible when the payload does not actually say.
//
// # Why the answer is empty so often, and why that is correct
//
// Only two payload shapes carry a ledger identifier at all:
//
//	*model.Ledger / model.Ledger   → LedgerID. The event is about the ledger itself.
//	*model.Balance / model.Balance → LedgerID. A balance belongs to exactly one ledger.
//
// Every other event type returns "", and each for a concrete reason rather than an
// oversight:
//
//   - TRANSACTIONS. model.Transaction HAS NO LEDGER FIELD. A transaction's ledger
//     association is indirect, through the balances it moves value between, so
//     resolving it would require reading one of those balances from the database — on
//     the event-capture path, inside the caller's open ledger transaction, for a value
//     nothing needs to route by. That cost is not worth paying, and INVENTING a value
//     is worse: the previous code put the SOURCE BALANCE ID in the ledger column,
//     where it read as a ledger id to everything downstream.
//   - BALANCE MONITORS. model.BalanceMonitor carries a balance and a condition and no
//     ledger.
//   - IDENTITIES. model.Identity carries no ledger; an identity is not scoped to a
//     ledger in this model.
//   - BULK TRANSACTION BATCHES. A batch is a runtime grouping of work, not a ledger
//     object, and its transactions may span ledgers.
//   - SYSTEM ERRORS. There is no aggregate of any kind, let alone a ledger.
//
// The column is nullable precisely so this can be stated: NULL means "this event has
// no ledger" as a fact, which a NOT NULL column defaulting to the empty string could
// not distinguish from "nobody looked".
//
// Parameters:
//   - payload interface{}: the NewWebhook payload object. May be nil or of any type.
//
// Returns:
//   - string: the ledger id, trimmed, or "" when the payload carries none.
func eventLedgerID(payload interface{}) string {
	switch typed := payload.(type) {
	case *model.Ledger:
		if typed == nil {
			return ""
		}
		return strings.TrimSpace(typed.LedgerID)
	case model.Ledger:
		return strings.TrimSpace(typed.LedgerID)
	case *model.Balance:
		if typed == nil {
			return ""
		}
		// No fallback to BalanceID here, unlike the partition key. A balance id is not
		// a ledger id, and returning one would put exactly the wrong kind of value in
		// the ledger column again — which is the defect this split exists to fix.
		return strings.TrimSpace(typed.LedgerID)
	case model.Balance:
		return strings.TrimSpace(typed.LedgerID)
	default:
		return ""
	}
}

// transactionPartitionKey applies the transaction key preference documented on
// eventPartitionKey: source balance, then destination balance, then the transaction
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
// # The ONE nil-nil case, and the one error case
//
// (nil, nil) means "there is nothing to capture" and has exactly one cause: event
// publishing is not configured. Reproducing SendWebhook's no-op-when-unconfigured
// contract is what lets Blnk run with no notification sink; see
// eventPublishingConfigured. A nil row is safe for every consumer — the atomic
// writers' variadic event parameter skips nil entries, and publishEvent treats nil as
// "nothing to persist".
//
// A payload that CANNOT BE MARSHALED is an ERROR, and this is a deliberate change from
// the earlier behaviour of logging it and returning nil.
//
// The old reasoning was that a malformed payload must never take down a ledger write,
// and that propagating an error would reject a financially valid mutation because of a
// notification defect. The trade it actually made was worse than the one it avoided:
// the mutation committed, the event was silently gone, the caller was told everything
// had succeeded, and NOTHING downstream could ever discover the loss — not the outbox,
// not the relay, not the dead-letter inventory, not the daily reconciliation, which
// counts rows that exist and cannot count a row that was never written. An event that
// no mechanism can find is indistinguishable from an event that was never produced.
//
// Returning the error hands the decision to the caller, which is the only place it can
// be made correctly. On the in-transaction path the caller's mutation rolls back, which
// is what requirement R-2 asks for: a mutation whose event cannot be captured must not
// commit. On the standalone path the caller reports the failure.
//
// The practical risk this adds is small and bounded, which is what makes the trade
// right rather than merely principled. json.Marshal fails on channels, functions and
// cyclic structures; the payloads reaching here are model structs and small
// string-keyed maps built in this repository, none of which contain any of those. A
// failure here is a programming defect in a new producer, and the loudest possible
// moment to learn about it is the first time that producer runs.
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
//   - *model.EventOutbox: the row to persist, or nil when publishing is unconfigured.
//   - error: nil on success and on the unconfigured no-op; a typed internal-server
//     error when the payload cannot be marshaled.
//
// EventOption supplies a fact about an event that the payload cannot yield.
//
// It is variadic at every entry point deliberately. The transport substitution at the
// producer call sites must stay a one-identifier edit — SendWebhook becomes PublishEvent —
// because that is what makes the payload-preservation guarantee verifiable by reading the
// diff. A variadic option list keeps every one of those call sites source-compatible while
// giving the sites that KNOW something the payload does not a way to state it.
//
// There is exactly one option today, WithEventLedgerID.
type EventOption func(*eventAttributes)

// eventAttributes carries what a caller supplied, before any derivation runs.
//
// It is a struct rather than a bare string so that adding a second caller-supplied
// attribute is an additive change here instead of a new parameter everywhere.
type eventAttributes struct {
	// ledgerID is the ledger the mutation belonged to, as supplied by the caller.
	// Empty means "not supplied", which sends the derivation on to the payload.
	ledgerID string
}

// WithEventLedgerID supplies THE LEDGER THE MUTATION BELONGED TO.
//
// # Why a caller has to supply it at all
//
// model.Transaction HAS NO LEDGER FIELD. A transaction's ledger association is indirect,
// through the balances it moves value between, so a transaction payload simply cannot yield
// the ledger — and MetaData is caller-controlled and unfit to derive a routing key from.
// Without this option every transaction event stores ledger_id as SQL NULL, which is
// honest but leaves no way for a consumer, an operator or the daily reconciliation to group
// a transaction event by ledger at all.
//
// Every producer that performs a ledger-scoped mutation DOES know it: transaction
// execution has already loaded the source and destination balances, each of which carries
// LedgerID, and the balance and ledger post-action hooks hold the entity itself.
//
// # It becomes the PARTITION KEY as well as the recorded ledger, and what that costs
//
// Requirement R-6 partitions by ledger id, and eventPartitionKey already keys by the ledger
// for every payload that yields one — a ledger event by its own id, a balance event by its
// ledger. A caller supplying the ledger is supplying exactly what the payload could not, so
// it takes the same precedence; doing otherwise would make one rule apply to balances and a
// different one to transactions.
//
// THE CONSEQUENCE IS EXPLICIT, not a side effect: every event of one ledger then lands on
// ONE partition. That is the strongest ordering guarantee available and it is what R-6 asks
// for, and it also means a deployment whose volume is concentrated in a single ledger reads
// that topic through a single partition however many the topic has. Omitting the option
// leaves a transaction event keyed on its source balance, which matches what the
// transaction queue already shards on (hashBalanceID(transaction.Source) in queue.go),
// spreads load across partitions, and preserves per-BALANCE rather than per-LEDGER
// ordering — strictly weaker for the ledger, strictly better for parallelism. Neither is
// wrong; the choice belongs to whoever wires the call site, which is why this is an option
// and not a derivation.
//
// A blank or whitespace-only value is IGNORED rather than stored, because a whitespace key
// hashes to a different partition from an empty one and would split one ledger's events
// across two partitions — the precise failure this mechanism exists to prevent.
//
// Parameters:
//   - ledgerID string: the ledger the mutation belonged to.
//
// Returns:
//   - EventOption: applied by PrepareEventOutbox, PublishEvent and PublishEventInTx.
func WithEventLedgerID(ledgerID string) EventOption {
	return func(attributes *eventAttributes) {
		if trimmed := strings.TrimSpace(ledgerID); trimmed != "" {
			attributes.ledgerID = trimmed
		}
	}
}

// applyEventOptions folds a caller's options into a fresh attribute set.
//
// A nil option is skipped rather than panicking: the list is variadic and assembled at call
// sites that may build it conditionally, and a nil entry there is a caller's slip that must
// not take a ledger write down with it.
//
// Parameters:
//   - options []EventOption: the caller's options, possibly empty or containing nils.
//
// Returns:
//   - eventAttributes: the folded attributes.
func applyEventOptions(options []EventOption) eventAttributes {
	var attributes eventAttributes
	for _, option := range options {
		if option != nil {
			option(&attributes)
		}
	}

	return attributes
}

func (l *Blnk) PrepareEventOutbox(ctx context.Context, event NewWebhook, options ...EventOption) (*model.EventOutbox, error) {
	_, span := tracer.Start(ctx, "PrepareEventOutbox")
	defer span.End()

	cnf := l.eventConfiguration()
	if !eventPublishingConfigured(cnf) {
		span.AddEvent("Event publishing not configured")
		return nil, nil
	}

	// THE PAYLOAD GUARANTEE: marshal the whole NewWebhook value, both keys, exactly
	// as SendWebhook does. Never the inner payload alone, never re-shaped, never
	// re-keyed.
	payloadBytes, err := json.Marshal(event)
	if err != nil {
		logrus.WithError(err).WithField("event_type", event.Event).
			Error("event not captured: its payload could not be marshaled")
		span.RecordError(err)

		return nil, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"The event payload could not be serialized",
			fmt.Errorf("blnk: marshalling the payload of event %q: %w", event.Event, err),
		)
	}

	eventType := strings.TrimSpace(event.Event)
	aggregateID := eventAggregateID(event.Payload)

	// TWO DIFFERENT VALUES, resolved from two different functions, stored in two
	// different columns. Conflating them is the defect this split exists to fix; see
	// eventPartitionKey and eventLedgerID.
	partitionKey := eventPartitionKey(event.Payload)
	ledgerID := eventLedgerID(event.Payload)

	// A CALLER-SUPPLIED LEDGER WINS over both derivations, and over each for its own
	// reason. It is the authoritative ledger, so it is what ledger_id must record —
	// a payload that yields none (a transaction) would otherwise store SQL NULL. And
	// it is the R-6 partitioning dimension, so it takes the same precedence for the
	// key that a ledger derived FROM the payload already takes: see WithEventLedgerID
	// for the ordering-versus-parallelism trade this makes explicit.
	if supplied := applyEventOptions(options).ledgerID; supplied != "" {
		ledgerID = supplied
		partitionKey = supplied
	}

	// The documented fallback chain, and it applies to the PARTITION KEY ONLY. Its
	// purpose is a key that is always present and always deterministic: an empty key
	// would let Kafka scatter the event round-robin and destroy ordering with nothing
	// in the data to show it.
	//
	// Falling back to the event type is what gives system.error — which has no
	// aggregate of any kind — a single partition and therefore a total order, which
	// is precisely what an error stream wants. The sentinel is reached only when
	// there is no event type either.
	//
	// NOTHING LIKE THIS APPLIES TO ledgerID. A missing ledger stays missing: it is
	// stored as SQL NULL, because a fabricated ledger id is worse than no ledger id.
	if partitionKey == "" {
		partitionKey = aggregateID
	}
	if partitionKey == "" {
		partitionKey = eventType
	}
	if partitionKey == "" {
		partitionKey = unkeyedEventPartitionKey
	}
	// aggregate_id is NOT NULL in the schema and is what consumers group by, so it
	// inherits the partition key once every payload-derived candidate is exhausted.
	if aggregateID == "" {
		aggregateID = partitionKey
	}

	// The event id is DERIVED when the event has a stable identity, and random when it
	// does not.
	//
	// event_id carries two contracts at once: it is the subscriber's idempotency key,
	// and it is the unique index that makes the outbox's write side exactly-once. A
	// freshly random id satisfies neither for a RETRY — a mutation replayed after an
	// ambiguous failure prepares its event a second time, the index sees a different
	// key, admits a second row, and the subscriber receives one business event twice
	// with no way to tell. It also makes the CONFLICT arm of
	// wrapEventOutboxInsertError unreachable, so the discrimination it performs on the
	// unique violation — which exists precisely so a caller retrying a captured
	// mutation can recognise it and carry on — could never fire.
	//
	// Determinism is applied ONLY where it is correct. model.EventIdentityFor decides,
	// and it refuses the repeatable events: balance.monitor fires every time its
	// condition is met, and system.error is emitted per occurrence, so deriving either
	// would collapse every later occurrence into a duplicate the index rejects and the
	// pipeline would stop delivering them with no error anywhere.
	eventID := model.NewEventID()
	if identity, derivable := model.EventIdentityFor(eventType, event.Payload); derivable {
		eventID = model.DeriveEventID(identity, eventType, model.SchemaVersionV1)
	}

	outbox := &model.EventOutbox{
		EventID:      eventID,
		EventType:    eventType,
		AggregateID:  aggregateID,
		PartitionKey: partitionKey,
		LedgerID:     ledgerID,
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

	// AN UNCATALOGUED EVENT TYPE IS A DEFECT SIGNAL, and this is where it is raised.
	//
	// model.EventCategory routes an event type it does not recognise to the internal
	// system category rather than dropping it, which is what keeps the zero-exceptions
	// coverage guarantee true — but the routing is a fallback, not a decision anybody
	// made about that event's audience. Saying so here, once per captured row, is what
	// turns "events are arriving on the system topic" into "this producer was added
	// without extending model.EventCategory", which is the actual fix.
	//
	// It is deliberately at WARNING and deliberately unguarded by a level check: it
	// cannot fire for any event type this repository catalogues, so a healthy
	// deployment never emits it at all.
	if !model.IsCataloguedEventType(outbox.EventType) {
		logrus.WithFields(logrus.Fields{
			"event_id":   outbox.EventID,
			"event_type": outbox.EventType,
			"topic":      outbox.Topic,
		}).Warn(
			"event capture: this event type is not in the event catalogue, so it was routed to " +
				"the internal system topic, which no subscriber can be granted. Add it to " +
				"model.EventCategory so it reaches the audience it belongs to",
		)
	}

	span.AddEvent("Event outbox entry prepared", trace.WithAttributes(
		attribute.String("event.id", outbox.EventID),
		attribute.String("event.type", outbox.EventType),
		attribute.String("event.partition_key_hash", hashLogIdentifier(outbox.PartitionKey)),
		attribute.String("event.topic", outbox.Topic),
	))

	return outbox, nil
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
func (l *Blnk) PublishEvent(ctx context.Context, event NewWebhook, options ...EventOption) error {
	return l.publishEvent(ctx, nil, event, options...)
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
func (l *Blnk) PublishEventInTx(ctx context.Context, tx *sql.Tx, event NewWebhook, options ...EventOption) error {
	return l.publishEvent(ctx, tx, event, options...)
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
// # The legacy-only branch, taken BEFORE anything is captured
//
// A deployment with a webhook URL and no brokers has no outbox reader — the relay refuses
// the no-op publisher — so capturing a row there would be capturing a row that nothing
// can ever drain. Such a deployment is routed straight to SendWebhook, which is the exact
// call every producer made before this feature existed, so its behaviour is unchanged
// (AAP §0.5.4, "KAFKA_BROKERS empty → legacy path unchanged"). See
// legacyWebhookOnlyDelivery and deliverLegacyWebhookOnly.
//
// The branch is taken ONLY when tx is nil. An enqueue cannot be rolled back with the
// caller's transaction, so on the in-transaction path this deployment captures nothing and
// delivers nothing, and the caller's post-commit path delivers instead.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
//   - tx *sql.Tx: the caller's transaction, or nil for the standalone insert.
//   - event NewWebhook: the event to capture.
//   - options ...EventOption: caller-supplied facts the payload cannot yield, forwarded
//     verbatim to PrepareEventOutbox.
//
// Returns:
//   - error: nil on success and on every no-op; the persistence error otherwise.
func (l *Blnk) publishEvent(ctx context.Context, tx *sql.Tx, event NewWebhook, options ...EventOption) error {
	ctx, span := tracer.Start(ctx, "PublishEvent")
	defer span.End()

	// THE LEGACY-ONLY PATH, and it is why a webhook-only deployment keeps working.
	//
	// With no Kafka broker configured there is no relay — it refuses to start rather than
	// retire an outbox it cannot publish — so a row captured here would never be claimed
	// and never be delivered. Before this branch existed that is exactly what happened:
	// the producers had stopped calling SendWebhook, the rows accumulated, and the
	// deployment's notifications stopped with nothing failing to say so.
	//
	// So when Kafka is absent and a webhook URL is present, the event goes straight down
	// the transport it has always gone down. This is the pre-migration behaviour restored
	// verbatim, including its error contract: SendWebhook's enqueue error reaches the
	// caller exactly where it used to.
	//
	// The ORDER of the two checks is the important part. Kafka being configured wins, so a
	// deployment that has opted in captures to the outbox and lets the relay drive BOTH
	// legs from that one row — which is what makes the two transports carry identical
	// bytes during the dual-delivery window. This branch can only be reached when there is
	// no outbox path at all.
	//
	// AND ONLY WHEN THE CALLER HOLDS NO TRANSACTION. asynq is backed by Redis, so an
	// enqueue cannot join the caller's PostgreSQL transaction and cannot be rolled back
	// with it: taking this branch from PublishEventInTx would deliver a webhook describing
	// a mutation that may still roll back, which is the exact hazard the transactional
	// outbox exists to remove — and it would do it on the one path that promised not to.
	// So an in-transaction capture on a Kafka-less deployment is a documented no-op, and
	// the caller's POST-COMMIT path is what delivers the event. That is what
	// publishEntityEventWhenUncaptured does at the three entity producers, and what the
	// atomic transaction writers' callers do for the rest.
	if cnf := l.eventConfiguration(); tx == nil && !eventPublishingConfigured(cnf) && legacyWebhookOnly(cnf) {
		span.AddEvent("Event delivered over the legacy webhook transport; no Kafka broker is configured")

		if l == nil {
			return nil
		}

		// The queue client is what the legacy transport enqueues onto, and a nil one means
		// there is no transport at all on a deployment that asked for one. Reported as an
		// error rather than skipped, for the same reason the missing-datasource arm below
		// is: every event would be dropped, permanently and invisibly, while every caller
		// was told it had succeeded.
		if l.asynqClient == nil {
			err := apierror.NewAPIError(
				apierror.ErrInternalServer,
				"A legacy webhook URL is configured but no queue client is available to deliver the event",
				fmt.Errorf("blnk: event %q cannot be delivered: the Blnk instance has no asynq client", event.Event),
			)
			logrus.WithField("event_type", event.Event).WithError(err).
				Error("event not delivered: a webhook URL is configured but this Blnk instance has no queue client")
			span.RecordError(err)

			return err
		}

		return l.SendWebhook(event)
	}

	outbox, err := l.PrepareEventOutbox(ctx, event, options...)
	if err != nil {
		// A payload that will not serialise is a defect, not a transient condition, and
		// it is returned rather than swallowed so the caller — and, on the
		// in-transaction path, the caller's rollback — can act on it.
		span.RecordError(err)
		return err
	}
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
	// marshal and no I/O, and it is what gives this failure the event identity that
	// makes it actionable — "some event was dropped" is not a diagnosable message.
	// The nil-receiver arm is evaluated first so the field access can never run on a
	// nil pointer.
	//
	// THIS IS AN ERROR, not a warning, and that is a deliberate change. Reaching here
	// means publishing IS configured — PrepareEventOutbox already returned nil for the
	// unconfigured case — and there is nowhere to persist to. Every event in such a
	// deployment is dropped, permanently and invisibly, while every caller is told it
	// succeeded. Returning nil made that a log line nobody reads; returning the error
	// makes it a failure the caller reports and, inside a ledger transaction, rolls
	// back over.
	//
	// NewBlnk(nil) remains a supported construction: with no brokers configured it
	// never reaches this line, so tests and deployments that run without a datasource
	// and without Kafka are unaffected.
	if l == nil || l.datasource == nil {
		err = apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Event publishing is configured but no datasource is available to capture the event",
			fmt.Errorf("blnk: event %q (%s) cannot be captured: the Blnk instance has no datasource",
				outbox.EventID, outbox.EventType),
		)
		logrus.WithFields(logrus.Fields{
			"event_id":   outbox.EventID,
			"event_type": outbox.EventType,
			"topic":      outbox.Topic,
		}).WithError(err).Error("event not captured: publishing is configured but this Blnk instance has no datasource")
		span.RecordError(err)

		return err
	}

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
		// Logged here for immediate operator visibility, and returned so the caller's
		// existing error handling — which routes to notification.NotifyError at most
		// call sites — behaves exactly as it did with SendWebhook.
		//
		// The aggregate id is HASHED rather than printed. It is a ledger, balance,
		// transaction or identity id: a financial identifier naming whose money this
		// event is about, and this line is emitted on a failure path that a broker
		// outage can make high-volume. Correlation does not need the plaintext —
		// event_id identifies the event uniquely and is already here — and the token
		// still lets an operator see that several failures share one aggregate, which
		// is the only thing the identifier was contributing.
		logrus.WithFields(logrus.Fields{
			"event_id":          outbox.EventID,
			"event_type":        outbox.EventType,
			"topic":             outbox.Topic,
			"aggregate_id_hash": hashLogIdentifier(outbox.AggregateID),
			"in_transaction":    tx != nil,
		}).WithError(err).Error("failed to record event in the outbox")
		span.RecordError(err)

		return err
	}

	span.AddEvent("Event recorded in outbox", trace.WithAttributes(
		attribute.Int64("event.outbox_id", outbox.ID),
	))

	return nil
}

// deliverLegacyWebhookOnly delivers an event over the legacy HTTP transport on a
// deployment that has no Kafka, by making exactly the call every producer made before
// this feature existed.
//
// # Why this exists rather than an outbox row
//
// The outbox has one reader, EventRelayProcessor, and it refuses to run without a real
// Kafka publisher: the no-op reports every publish as dispatched, so a relay given it
// would mark the whole outbox dispatched having sent nothing. On a webhook-only
// deployment there is therefore nothing that can ever claim a captured row. Capturing one
// anyway is the worst of both worlds — the row accumulates forever AND the webhook the
// deployment is configured for is never sent — which is precisely the regression this
// function removes. The AAP's state machine says the legacy path is UNCHANGED when
// KAFKA_BROKERS is empty (§0.5.4), and SendWebhook is what unchanged means.
//
// # The in-transaction path deliberately delivers nothing
//
// SendWebhook enqueues an asynq task, and asynq is backed by Redis, which cannot be
// enrolled in a PostgreSQL transaction. Enqueuing from inside the caller's open
// transaction would deliver a webhook for a mutation that then rolled back — a webhook
// asserting money moved when it did not. That is strictly worse than not delivering, so
// the in-transaction path returns nil and the caller's POST-COMMIT path delivers instead.
//
// That is not a gap in this codebase's own flows: the transaction execution path receives
// a nil row from PrepareEventOutbox on an unconfigured deployment, so eventCaptured stays
// false and postTransactionActions publishes through the standalone path, which arrives
// back here with tx == nil and delivers. It IS a constraint on any future caller of
// PublishEventInTx, which is why the trace records the skip and why the method's own
// documentation states it.
//
// Every other property of the legacy path is inherited from SendWebhook untouched: it
// no-ops on an empty URL, it marshals the same NewWebhook value the HTTP body has always
// been, and asynq owns the delivery retry.
//
// Parameters:
//   - ctx context.Context: the context for the operation, used for tracing only —
//     SendWebhook takes none.
//   - tx *sql.Tx: the caller's transaction, or nil. Non-nil selects the skip described
//     above.
//   - event NewWebhook: the event name and payload object, forwarded verbatim.
//
// Returns:
//   - error: whatever SendWebhook reports, or nil on the in-transaction skip.
func (l *Blnk) deliverLegacyWebhookOnly(ctx context.Context, tx *sql.Tx, event NewWebhook) error {
	_, span := tracer.Start(ctx, "DeliverLegacyWebhookOnly")
	defer span.End()

	span.SetAttributes(
		attribute.String("event.type", event.Event),
		attribute.Bool("event.in_transaction", tx != nil),
		attribute.Bool("event.legacy_only", true),
	)

	if tx != nil {
		span.AddEvent(
			"Legacy-only delivery skipped inside a database transaction; the caller's " +
				"post-commit path delivers it",
		)

		return nil
	}

	if l == nil {
		return nil
	}

	// SendWebhook enqueues through l.asynqClient and would panic on a nil one. NewBlnk
	// always builds it from the Redis DSN, so this is unreachable in a real deployment;
	// it is reachable from a hand-assembled instance, and a panic inside a post-action
	// goroutine takes the process down rather than surfacing as an error. The error is
	// returned rather than swallowed for the same reason the nil-datasource branch above
	// returns one: the deployment asked for webhooks and is not getting them.
	if l.asynqClient == nil {
		err := apierror.NewAPIError(
			apierror.ErrInternalServer,
			"The legacy webhook transport is configured but no queue client is available to enqueue the delivery",
			fmt.Errorf("blnk: event %q cannot be delivered: the Blnk instance has no asynq client", event.Event),
		)
		logrus.WithFields(logrus.Fields{
			"event_type":  event.Event,
			"legacy_only": true,
		}).WithError(err).Error("event not delivered: the legacy webhook transport has no queue client")
		span.RecordError(err)

		return err
	}

	if err := l.SendWebhook(event); err != nil {
		// Logged and returned, exactly as the outbox branch does, so a call site's
		// existing routing to notification.NotifyError behaves identically whichever
		// transport is in use.
		logrus.WithFields(logrus.Fields{
			"event_type":  event.Event,
			"legacy_only": true,
		}).WithError(err).Error("failed to enqueue the legacy webhook delivery")
		span.RecordError(err)

		return err
	}

	span.AddEvent("Event delivered over the legacy webhook transport")

	return nil
}
