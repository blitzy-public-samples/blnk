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
	"errors"
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

// NewWebhook is the webhook notification envelope, and it is a FROZEN CONTRACT.
// It includes an event type and associated payload data.
//
// IT LIVES HERE, NOT IN webhooks.go, AND THAT IS THE WHOLE POINT. Marshaled, it is the
// exact HTTP body Blnk has always POSTed to a subscriber: a two-key object,
// {"event": <string>, "data": <object>}. That same marshaled object is carried verbatim —
// both keys, unaltered — as the `payload` member of the Kafka LedgerEvent envelope, which
// is what lets an existing subscriber's body parser keep working when only the transport
// has changed. So the struct is the PAYLOAD CONTRACT, not part of the HTTP transport, and
// it has to outlive the file that transport lives in.
//
// It was declared in webhooks.go and has been relocated here ahead of that file's
// deletion, which is STEP 1 of the sunset procedure at the foot of webhooks.go and the
// order the specification requires: relocate first, delete second. Twelve surviving
// non-test files depend on this type — blnk.go, ledger.go, identity.go, balance.go,
// transaction_execution.go, transaction_bulk.go, transaction_rejection.go,
// event_monitor_handoff.go and this file among them — so removing it with the transport
// would have taken the payload contract down with the delivery mechanism.
//
// Consequently the field names, the field types and above all the JSON tags must not
// change. Renaming a tag, dropping the outer envelope in favour of the inner data
// object, or adding a field would silently break byte-equivalence between the two
// transports and, with it, every subscriber parser written against the HTTP era.
type NewWebhook struct {
	Event   string      `json:"event"` // The event type that triggered the webhook.
	Payload interface{} `json:"data"`  // The data associated with the event.
}

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

// THE UNKEYED SENTINEL IS GONE, and its removal is the point of this note.
//
// The key decides the partition, and the partition decides ordering: every event sharing
// a key is appended to one partition and is therefore consumed in publish order. An EMPTY
// key is the failure mode — Kafka treats a null/empty key as "any partition", so unkeyed
// events are scattered round-robin and their relative order is lost silently, with nothing
// in the data to show that it happened.
//
// A constant named unkeyedEventPartitionKey used to absorb that case: an event with no
// aggregate AND no event type was given the literal key "blnk.unkeyed" and admitted. That
// traded a visible refusal for an invisible one. The row was accepted, published, and
// ordered only against other unkeyable events — a guarantee no subscriber asked for and
// none could use — and the only way to discover it was to think to run
// `WHERE partition_key = 'blnk.unkeyed'`.
//
// PrepareEventOutbox now REFUSES such an event with ErrEventKeyUnresolvable. Reaching it
// requires a producer to pass an event with no type and a payload with no identifier of any
// kind — a zero-valued NewWebhook — which is a producer defect that must fail at the call
// site rather than become a row. Every real event type has a declared key dimension (see
// model.KeyDimensionForEventType) and every real payload yields at least the event type,
// so nothing that this repository emits can reach the refusal.

// postCommitEventPublishSem bounds how many post-commit event captures may be in
// flight at once, across every transaction and balance in the process.
//
// # What is unbounded without it
//
// The post-commit hooks that publish an event spawn one goroutine per occurrence and
// nothing else limits how many can exist together. That was survivable while such a
// goroutine only enqueued into Redis, and stopped being survivable once it also had to
// reach PostgreSQL: at 500 events per second against a database that has begun to
// stall, the arrival rate is fixed and the completion rate is not, so goroutines
// accumulate without limit, each holding a connection request, until the process is
// killed for memory. Nothing in the logs says why, because nothing failed.
//
// The balance-monitor fan-out is the worst shape of it. One balance can carry many
// monitors and many of them can fire on one update, so the number of goroutines is a
// PRODUCT of two counts rather than one per transaction.
//
// # Acquiring in the caller is the point
//
// The permit is taken BEFORE the goroutine is spawned, so a saturated database is felt
// by the producer as backpressure rather than absorbed as unbounded queueing. That is
// what balanceMonitorSem already does for the monitor checks themselves, for the same
// reason: the only safe response to work arriving faster than it can be completed is to
// slow the arrivals down.
//
// # Why 64
//
// The work behind one permit is a single indexed INSERT — single-digit milliseconds on a
// healthy system — so 64 in flight clears far more than the 500 events per second
// requirement V-1 states. It is deliberately larger than balanceMonitorSem's 32 because
// a monitor check is a read that can be deferred, while this work carries an event
// capture that must not queue behind unrelated reads.
//
// It lives here rather than beside balanceMonitorSem because it belongs to the event
// pipeline: the transaction pipeline's own files are frozen by AAP §0.6.2, and a bound
// that this feature introduced belongs in a file this feature owns.
var postCommitEventPublishSem = make(chan struct{}, 64)

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
	// Delegated rather than reimplemented. The database layer asks the SAME question
	// when it decides whether to write a balance-monitor handoff inside a ledger
	// transaction, and the two answers must be identical: a disagreement would either
	// write handoffs nothing drains, or leave a movement whose monitors neither the
	// handoff nor the post-commit path evaluates. One implementation makes that
	// unrepresentable. See config.Configuration.EventPublishingConfigured.
	return cnf.EventPublishingConfigured()
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

// resolveEventPartitionKey picks the Kafka message key and reports WHICH dimension it came
// from.
//
// The second return value is what makes a departure from requirement R-6 detectable. The
// three candidates are tried in descending strength and the dimension is named alongside the
// answer, so PrepareEventOutbox can compare it against the dimension the event type declares
// (model.KeyDimensionForEventType) instead of accepting whatever the chain produced.
//
// Precedence, and why it is this order:
//
//  1. derived — the key eventPartitionKey resolved from the payload. That function applies
//     the R-6 rule itself: a payload that knows its ledger returns the ledger, whatever its
//     category. So a non-empty answer here is the ledger for every ledger-bearing event and
//     the event's own aggregate otherwise, which is exactly what the two strong dimensions
//     mean.
//  2. aggregate — the aggregate id resolved separately by eventAggregateID, for a payload
//     shape eventPartitionKey has no arm for but that still names something.
//  3. eventType — the type itself, which is what gives system.error a single partition and
//     therefore a total order.
//
// The dimension reported for candidates 1 and 2 is ledger when the caller's derived key IS
// the ledger and aggregate otherwise, which the caller decides by passing the ledger it
// resolved; this function only distinguishes "from the payload" from "from the type". That
// split is deliberate: the ledger-versus-aggregate question is answered by eventLedgerID,
// and duplicating it here would create a second answer able to disagree.
//
// Parameters:
//   - derived string: the key resolved from the payload, or the caller-supplied ledger when
//     one was given. Empty when the payload yielded nothing.
//   - aggregateID string: the separately resolved aggregate id. Empty when there is none.
//   - eventType string: the trimmed event name. Empty only for a zero-valued event.
//   - ledgerID string: the authoritative ledger, empty when the event has none. Used ONLY to
//     name the dimension, never as a candidate — a caller-supplied ledger has already been
//     folded into derived by the caller.
//
// Returns:
//   - string: the chosen key, empty when every candidate is empty.
//   - model.EventKeyDimension: the dimension the key came from. Meaningless when the key is
//     empty, and the caller refuses that case.
func resolveEventPartitionKey(
	derived, aggregateID, eventType, ledgerID string,
) (string, model.EventKeyDimension) {
	if derived != "" {
		if ledgerID != "" && derived == ledgerID {
			return derived, model.EventKeyDimensionLedger
		}

		return derived, model.EventKeyDimensionAggregate
	}

	if aggregateID != "" {
		return aggregateID, model.EventKeyDimensionAggregate
	}

	if eventType != "" {
		return eventType, model.EventKeyDimensionEventType
	}

	return "", model.EventKeyDimensionEventType
}

// eventPartitionKey derives the KAFKA MESSAGE KEY for an event FROM ITS PAYLOAD ALONE.
//
// # IT IS THE FALLBACK, not the contract
//
// The contract is one rule — requirement R-6 partitions by LEDGER ID, and
// PrepareEventOutbox keys every ledger-scoped event on its ledger. That ledger arrives
// through WithEventLedgerID, supplied by the producer, and it OVERRIDES whatever this
// function returns. Every ledger-scoped producer supplies it: transaction execution
// (resolved from the loaded source balance, falling back to the destination), the
// balance post-action and monitor check, and the ledger post-action. So for
// transaction.*, balance.created, balance.monitor and ledger.created the wire key is
// the ledger, and this function's answer for those payloads is never what ships.
//
// What this function is for is the events that genuinely have NO ledger, and the one
// case where a ledger-scoped event cannot resolve one:
//
//   - identity.created — an identity is not scoped to a ledger in this model.
//   - bulk_transaction.<status> — a batch is a runtime grouping, not a ledger object.
//   - system.error — no aggregate of any kind; it reaches the caller's chain and keys
//     on the event type, giving the error stream a single partition and a total order.
//   - A REJECTED transaction, persisted with no balances at all: no balance moved, so
//     the producer supplies no ledger and the derivation below is what keys it.
//
// It is therefore ALSO the guarantee of last resort. A producer that forgets the
// option, or a payload whose ledger is unpopulated, still gets a stable, deterministic
// key rather than an empty one — and an empty key would let Kafka scatter the message
// round-robin and destroy ordering with nothing in the data to show it.
//
// # It is NOT the ledger column, and the two must never be conflated again
//
// This function was once named eventLedgerID and its result was stored in a column
// called ledger_id, which was wrong for most of the values it produced: anything
// reading ledger_id to learn which ledger an event belonged to got a BALANCE id for
// every transaction event and had no way to tell. The authoritative ledger now lives
// in its own column, populated by eventLedgerID or by the producer's option, and
// returns a value ONLY when the event genuinely has one. eventPartitionKey answers
// "where does this message go"; eventLedgerID answers "which ledger is this about".
//
// The key is hashed by a stable balancer to select a partition, so every event sharing
// a key lands on one partition and is consumed in publish order, while events with
// different keys carry no ordering relationship at all. Ordering is therefore not a
// property of the broker or of the relay — it is a property of the key being stable
// for every event belonging to one aggregate.
//
// Some events genuinely have no ledger, and for those the key is the event's OWN
// AGGREGATE. Nothing is fabricated: a ledger id that was not established is never
// invented, here or in the ledger_id column, because a plausible-looking ledger is worse
// than an absent one for everything that reads it afterwards.
//
//	*model.Ledger / model.Ledger
//	    LedgerID — the event is about the ledger itself, so the two answers coincide.
//
//	*model.Balance / model.Balance
//	    LedgerID, falling back to BalanceID. A balance belongs to exactly one ledger,
//	    so the ledger arm above answers first; the fallback covers a balance payload
//	    whose ledger field is not populated.
//
//	model.BalanceMonitor / *model.BalanceMonitor
//	    BalanceID, falling back to MonitorID. BalanceMonitor carries no ledger
//	    field, so this is the fallback for a monitor alert whose triggering
//	    balance had no ledger recorded — checkBalanceMonitors normally supplies
//	    the watched balance's ledger, and that is what ships. The watched balance
//	    is the closest stable aggregate otherwise: every alert for one balance
//	    stays ordered, which is what an alert consumer needs.
//
//	*model.Identity / model.Identity
//	    IdentityID. An identity is not scoped to a ledger in this model — model.Identity
//	    has no ledger field and an identity may be referenced by balances in several
//	    ledgers — so the identity is its own aggregate and ordering is per identity.
//
//	*model.Transaction / model.Transaction
//	    Source, falling back to Destination, then TransactionID.
//	    REACHED ONLY WHEN THE PRODUCER SUPPLIED NO LEDGER, which in practice means a
//	    rejected transaction persisted with no balances — every other transaction
//	    event is keyed on its ledger by WithEventLedgerID, per R-6. The source
//	    balance is the fallback because it is exactly what the existing transaction
//	    queue shards on — hashBalanceID(transaction.Source) in queue.go — so a
//	    fallback-keyed event still agrees with the ordering the ledger already
//	    imposes on that balance, and Source is stable across a transaction's whole
//	    lifecycle. Destination covers a credit-only transaction; TransactionID is
//	    the last resort for a multi-source parent whose own Source and Destination
//	    are unset.
//
//	map[string]interface{}
//	    batch_id, for the bulk_transaction.<status> batch summaries. A batch is a
//	    RUNTIME GROUPING rather than a ledger object and its members may span ledgers, so
//	    no single ledger is authoritative for the summary and choosing one would be a
//	    fabrication. The batch is the aggregate the summary describes, so keying by it is
//	    what keeps one batch's progress events ordered relative to one another. The
//	    batch's MEMBER transactions each carry their own ledger-keyed transaction.*
//	    event, so nothing about per-ledger ordering is lost.
//
// system.error and anything else reach the caller's fallback chain, which is
// documented at PrepareEventOutbox. NOTHING here returns a key that would leave the
// partition assignment to chance.
//
// # eventLedgerID answers a different question and must not be conflated with this one
//
// eventPartitionKey answers "which partition does this message go to"; eventLedgerID
// answers "which ledger is this event about", and it returns a value ONLY when the
// payload genuinely carries one. The two agree whenever a ledger is known — that is the
// R-6 rule above — and diverge only on the fallback, where this function still has to
// produce a stable key and the ledger column must stay NULL rather than hold a
// balance, identity or batch id. Storing one in the other was a real defect once: the
// ledger column held a source balance id for every transaction event, and nothing
// downstream could tell.
//
// Parameters:
//   - payload interface{}: the NewWebhook payload object. May be nil or of any type.
//
// Returns:
//   - string: the partition key, or "" when the payload carries none.
func eventPartitionKey(payload interface{}) string {
	// THE R-6 RULE, applied before any per-type fallback: an event that knows its
	// ledger is keyed by its ledger, whatever its category.
	if ledger := eventLedgerID(payload); ledger != "" {
		return ledger
	}

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
		return strings.TrimSpace(typed.BalanceID)
	case model.Balance:
		return strings.TrimSpace(typed.BalanceID)
	case *model.BalanceMonitor:
		if typed == nil {
			return ""
		}
		return firstNonBlank(typed.BalanceID, typed.MonitorID)
	case model.BalanceMonitor:
		return firstNonBlank(typed.BalanceID, typed.MonitorID)
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
// # WHAT THE PARTITION KEY RESOLVES TO, PER EVENT TYPE
//
// This is the authoritative statement of the resolution, because this function is where the
// two inputs meet: eventPartitionKey derives a key from the payload, and a ledger supplied
// through WithEventLedgerID overrides it. Reading either input alone gives the wrong answer,
// which is exactly how the documented table came to disagree with the code — it described the
// derivation and not the override.
//
// THE RULE IS ONE SENTENCE: every event that belongs to a ledger is keyed on that ledger, and
// requirement R-6 names the ledger as the partitioning dimension. The three event types that
// carry no key of that kind are keyed on the aggregate they describe, which is the only
// ordering domain they have — not a degraded fallback.
//
//	Event type                    Partition key                         ledger_id column
//	───────────────────────────── ───────────────────────────────────── ──────────────────
//	transaction.* (all seven)     the ledger, supplied by the producer  the same ledger
//	                              from the loaded source balance,
//	                              falling back to the destination's
//	bulk_transaction.<status>     the BATCH id                          SQL NULL
//	balance.created               the ledger                            the same ledger
//	balance.monitor               the ledger of the balance whose       the same ledger
//	                              update met the condition
//	identity.created              the IDENTITY id                       SQL NULL
//	ledger.created                the ledger id                         the same ledger
//	system.error                  the EVENT TYPE, so the whole error    SQL NULL
//	                              stream is one partition
//
// Why those three carry no ledger, in one line each: a bulk batch may name transactions whose
// balances sit in DIFFERENT ledgers, so "the ledger of this batch" is not a value that exists;
// model.Identity has no ledger field because the same party may hold balances in many ledgers
// or none; and system.error has no aggregate of any kind. See transaction_bulk.go, identity.go
// and the fallback chain below respectively.
//
// The table is mirrored in docs/event-streaming.md and pinned against the real function by
// TestPartitionKeyContract_MatchesTheDocumentedTable, which drives every row through this
// function rather than restating its logic — so the three statements cannot drift again.
//
// Parameters:
//   - ctx context.Context: the context for the operation, used for tracing only.
//   - event NewWebhook: the event name and payload object, passed through unchanged
//     from the producer call site.
//   - options ...EventOption: facts the payload cannot yield. WithEventLedgerID is the only
//     one, and it sets BOTH the ledger_id column and the partition key; see the table above.
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
	// identity is a caller-supplied stable identity for the event, used to DERIVE its
	// id. Empty means "not supplied", which sends the decision on to
	// model.EventIdentityFor. See WithEventIdentity.
	identity string
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
// # It becomes the PARTITION KEY as well as the recorded ledger, and that IS the contract
//
// Requirement R-6 partitions by ledger id. eventPartitionKey already keys by the ledger for
// every payload that yields one — a ledger event by its own id, a balance event by its
// ledger — and a caller supplying the ledger is supplying exactly what the payload could
// not, so it takes the same precedence. Doing otherwise would make one rule apply to
// balances and a different one to transactions, which is precisely the divergence between
// the published contract and the wire key that this documentation now rules out.
//
// THE CONSEQUENCE IS EXPLICIT, not a side effect: every event of one ledger lands on ONE
// partition. That is the strongest ordering guarantee available and it is what R-6 asks
// for, and it also means a deployment whose volume is concentrated in a single ledger reads
// that topic through a single partition however many the topic has.
//
// AND IT IS THE DELIVERED BEHAVIOUR, not merely an available one, which is why the
// subscriber-facing documentation states ledger keying as the RULE. Every ledger-scoped
// producer supplies the option: postTransactionActions and prepareTransactionEventOutbox for
// the status-derived transaction events, postLedgerActions and CreateLedger for
// ledger.created, postBalanceActions and CreateBalance for balance.created, and
// checkBalanceMonitors for balance.monitor. RejectTransaction is the one production site that
// deliberately does not, because a rejected transaction never loaded its balances and no
// ledger is known.
//
// Omitting the option leaves a transaction event keyed on its source balance, which matches
// what the transaction queue already shards on (hashBalanceID(transaction.Source) in
// queue.go), spreads load across partitions, and preserves per-BALANCE rather than
// per-LEDGER ordering — strictly weaker for the ledger, strictly better for parallelism.
// Neither is wrong; the choice belongs to whoever wires the call site, which is why this is
// an option and not a derivation. But a NEW ledger-scoped producer that omits it changes what
// docs/event-streaming.md promises, so add it there too rather than silently widening the
// fallback.
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

// WithEventIdentity supplies A STABLE IDENTITY THE PAYLOAD CANNOT YIELD, so the event's
// id is derived rather than random.
//
// # Why an option rather than a rule in the derivation table
//
// model.EventIdentityFor decides derivability from the event type and its payload alone,
// and for two event types it correctly answers "no". balance.monitor is one of them: a
// monitor fires every time its condition is met, so deriving from the monitor id would
// collapse every firing after the first into a duplicate the unique index rejects, and
// the alerts would silently stop. That answer is right for everything the payload knows.
//
// A CALLER can know more. The balance-monitor handoff mints an id per balance movement,
// so the pair (handoff, monitor) names exactly one alert — distinct across firings
// because each firing has its own handoff, and stable across re-evaluations of one
// handoff because the handoff id does not change. That is a genuine identity, and it is
// only available at the call site. Hard-coding it into the table would require the table
// to know about handoffs; supplying it here does not.
//
// # What it buys: duplicate suppression by the schema instead of by a lock
//
// A claim lease can lapse and let two processors evaluate one handoff, and a retry can
// re-run one whose commit was acknowledged too late. With a derived id both produce the
// SAME event_id, so the second insert collides with the unique index and the repository
// reports the existing row as success. The duplicate is absorbed structurally, which also
// covers a restart and a replay — none of which a lease would catch.
//
// # It takes precedence, and the precedence is the point
//
// A supplied identity wins over model.EventIdentityFor, including over its refusal to
// derive. That is the whole purpose: the caller is asserting an identity the payload
// could not express. It cannot make a derivable event LESS derivable, because a
// supplied identity is still an identity.
//
// A blank or whitespace-only value is IGNORED rather than used, so a caller that
// computes an identity conditionally and comes up empty falls back to the payload rule
// instead of deriving every such event from the same empty string — which would give
// them all one id and suppress all but the first.
//
// Parameters:
//   - identity string: the stable identity of this logical event.
//
// Returns:
//   - EventOption: applied by PrepareEventOutbox and the PublishEvent family.
func WithEventIdentity(identity string) EventOption {
	return func(attributes *eventAttributes) {
		if trimmed := strings.TrimSpace(identity); trimmed != "" {
			attributes.identity = trimmed
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
		withLoggableCause(logrus.WithField("event_type", event.Event), err).
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

	// TWO COLUMNS, ONE RULE. The key is the ledger wherever a ledger is known — that is
	// requirement R-6 and eventPartitionKey applies it before any per-type fallback — so
	// these two agree by construction for every ledger-bearing event. They are resolved
	// by two functions and stored in two columns because they diverge on the FALLBACK: an
	// event with no ledger still needs a stable key, while its ledger column must stay
	// NULL rather than hold a balance, identity or batch id. Conflating them is the defect
	// this split exists to fix; see eventPartitionKey and eventLedgerID.
	partitionKey := eventPartitionKey(event.Payload)
	ledgerID := eventLedgerID(event.Payload)

	// A CALLER-SUPPLIED LEDGER WINS over both derivations, and over each for its own
	// reason. It is the authoritative ledger, so it is what ledger_id must record —
	// a payload that yields none (a transaction) would otherwise store SQL NULL. And
	// it is the R-6 partitioning dimension, so it takes the same precedence for the
	// key that a ledger derived FROM the payload already takes: see WithEventLedgerID
	// for the ordering-versus-parallelism trade this makes explicit.
	attributes := applyEventOptions(options)
	if supplied := attributes.ledgerID; supplied != "" {
		ledgerID = supplied
		partitionKey = supplied
	}

	// THE KEY IS RESOLVED AGAINST A DECLARED DIMENSION, not through an untyped chain.
	//
	// model.KeyDimensionForEventType declares which identifier this event type's key is
	// SUPPOSED to be — the ledger for everything that describes ledger state, the event's
	// own aggregate for identities and batches, the event type itself for system.error —
	// and resolveEventPartitionKey reports which one was actually achieved. The two agree
	// for every event this repository emits with a populated payload; when they do not,
	// the miss is logged and recorded on the span rather than absorbed, because a key
	// taken from a balance instead of a ledger is a departure from requirement R-6 that
	// used to be indistinguishable from compliance.
	//
	// An event that can produce NO key is refused outright. See the note above the
	// removed unkeyed sentinel for why admitting it was worse than refusing it.
	//
	// NOTHING LIKE THIS APPLIES TO ledgerID. A missing ledger stays missing: it is
	// stored as SQL NULL, because a fabricated ledger id is worse than no ledger id.
	declared := model.KeyDimensionForEventType(eventType)
	partitionKey, achieved := resolveEventPartitionKey(partitionKey, aggregateID, eventType, ledgerID)
	if partitionKey == "" {
		err := fmt.Errorf(
			"blnk: event %q carries no ledger, no aggregate and no event type, so no Kafka "+
				"message key can be derived for it", event.Event,
		)

		withLoggableCause(logrus.WithFields(logrus.Fields{
			"declared_key_dimension": string(declared),
		}), err).Error(
			"event not captured: its Kafka message key is unresolvable, so publishing it would " +
				"scatter it across partitions and lose its ordering silently",
		)
		span.RecordError(err)

		return nil, apierror.NewAPIError(
			apierror.ErrEventKeyUnresolvable,
			"The event cannot be assigned a partition key",
			err,
		)
	}

	span.SetAttributes(
		attribute.String("event.key_dimension.declared", string(declared)),
		attribute.String("event.key_dimension.achieved", string(achieved)),
	)

	if achieved != declared {
		// REPORTED, NOT REFUSED. The one shape that reaches here in practice is a REJECTED
		// transaction persisted with no balances: no balance moved, so no ledger exists to
		// key it on, and refusing the event would destroy the only record that the
		// transaction was rejected. Keying it on its source balance — which is what the
		// transaction queue already shards on — keeps it ordered against that balance's
		// other events, which is the strongest guarantee available for it.
		//
		// The line is WARN rather than DEBUG because a ledger-dimensioned event type that
		// stops resolving its ledger is how per-ledger ordering degrades without any test
		// failing, and the event type plus both dimensions are what an operator needs to
		// find the producer.
		logrus.WithFields(logrus.Fields{
			"event_type":             eventType,
			"declared_key_dimension": string(declared),
			"achieved_key_dimension": string(achieved),
		}).Warn(
			"event keyed on a weaker dimension than its type declares: per-ledger ordering " +
				"is not available for this event, and its ledger column is NULL",
		)
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
	//
	// A CALLER MAY SUPPLY AN IDENTITY THE PAYLOAD CANNOT EXPRESS, and it takes
	// precedence — including over the refusal above. That is how balance.monitor becomes
	// derivable once the handoff exists: the pair (handoff, monitor) names one alert,
	// distinct across firings and stable across re-evaluations of one handoff. See
	// WithEventIdentity for why this belongs at the call site rather than in the table.
	eventID := model.NewEventID()
	if attributes.identity != "" {
		eventID = model.DeriveEventID(attributes.identity, eventType, model.SchemaVersionV1)
	} else if identity, derivable := model.EventIdentityFor(eventType, event.Payload); derivable {
		eventID = model.DeriveEventID(identity, eventType, model.SchemaVersionV1)
	}

	outbox := &model.EventOutbox{
		EventID:      eventID,
		EventType:    eventType,
		AggregateID:  aggregateID,
		PartitionKey: partitionKey,
		LedgerID:     ledgerID,
		// Resolved once, at construction, and stored on the row. The relay never
		// re-derives it, so a row stays bound to its ORIGINAL destination even if
		// KAFKA_TOPIC_PREFIX changes afterwards.
		//
		// STAYING BOUND IS NOT THE SAME AS STAYING PUBLISHABLE, and the difference is
		// one variable. The publisher refuses a topic outside the namespaces the
		// deployment declares, so after a rename these rows are publishable only while
		// the previous prefix is listed in KAFKA_HISTORICAL_TOPIC_PREFIXES. Without it
		// they are stranded: safe in the table, refused by the transport, and their
		// dead-letter writes and replays refused with them. AuditStrandedTopicPrefixes
		// is what makes that state visible, and cmd/server.go reports each finding at
		// start-up naming the prefix to declare.
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

	// THE CANONICAL EVENT VALUE IS PRODUCED ONCE, HERE, and everything downstream reuses
	// these bytes: the Kafka publish, the dead-letter copy (which splices failure metadata
	// onto them) and a dead-letter replay. Nothing re-marshals the envelope.
	//
	// That is what makes requirement R-5's byte-for-byte replay a property of the DATA
	// rather than of this serialiser never changing. Rebuilding the envelope at publish time
	// gave byte equality only within one build: add a member, reorder one, or upgrade the
	// encoder, and every row already captured would replay as different bytes, silently,
	// because the two values stay semantically equal.
	//
	// It is composed AFTER every envelope field above is final — the derived event id, the
	// resolved aggregate, the UTC occurrence instant and the schema version are all members
	// of it — so this line cannot move earlier.
	//
	// A failure here is the same failure the payload marshal above reports, on the same
	// terms: the payload is not valid JSON, which is a producer defect rather than a
	// transient condition, and the caller must see it.
	canonical, err := outbox.CanonicalEvent().CanonicalBytes()
	if err != nil {
		withLoggableCause(logrus.WithFields(logrus.Fields{
			"event_id":   outbox.EventID,
			"event_type": outbox.EventType,
		}), err).Error("event not captured: its canonical envelope could not be composed")
		span.RecordError(err)

		return nil, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"The event could not be serialized into its canonical envelope",
			fmt.Errorf("blnk: composing the canonical envelope of event %q: %w", outbox.EventID, err),
		)
	}
	outbox.EventRaw = canonical

	// THE TRACE THAT CAPTURED THIS EVENT IS WRITTEN ONTO THE ROW.
	//
	// This is the only moment at which it can be. The capture and the publish are decoupled by
	// design — the caller's transaction commits and returns, and the relay claims the row up to
	// a poll interval later, possibly in another process — so there is no in-memory context to
	// hand over and no way to recover the trace afterwards from anything but the row itself.
	// Without this, every request and database trace ENDED at the outbox insert and publishing,
	// retrying, dead-lettering and replaying one event each produced spans in unrelated traces.
	//
	// It is captured DELIBERATELY OUTSIDE the canonical envelope, after EventRaw is composed.
	// The envelope is the subscriber-facing contract and the basis of the byte-equality
	// guarantees (V-8 and V-9): adding a member that varies per request would change the stored
	// bytes for every event and break both. The trace travels on its own columns and reaches
	// subscribers as Kafka record HEADERS instead, which is where the OpenTelemetry messaging
	// conventions put it and which leaves the message body untouched.
	//
	// captureTraceContext yields empty strings when nothing is being traced, which is a
	// legitimate and common state rather than a fault.
	outbox.Traceparent, outbox.Tracestate = captureTraceContext(ctx)

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
	return l.publishEvent(ctx, nil, singleEventCaptureAttempt, event, options...)
}

// PostCommitEventCaptureContract is the SINGLE place the post-commit producers are described,
// and it exists because the alternative was three files each claiming to hold the only one.
//
// # What requirement R-2 binds, and what it does NOT get redefined into
//
// R-2 requires an event to be written to the outbox INSIDE THE SAME DATABASE TRANSACTION AS THE
// LEDGER MUTATION THAT PRODUCED IT. That is the contract, stated once, and nothing in this file
// may narrow it by reinterpreting its antecedent. Every event produced BY a ledger mutation is
// captured that way, with no exceptions — the three entity creations through their repositories'
// EventPreparer, and every transaction lifecycle event inside the transaction that records the
// transaction and moves the balances.
//
// # The three producers whose event comes into existence AFTER their mutation committed
//
// Three event types are produced by an OBSERVATION OVER STATE THAT IS ALREADY COMMITTED rather
// than by a mutation, so at the instant the event exists there is no open transaction left to
// insert it in. That is a fact about when the event can be WRITTEN. It is NOT a licence to say
// the event is outside R-2, and two of the three close the gap from the other side: the
// mutation's own transaction commits everything that DECIDES the event, so the deferred write
// cannot change which events exist or what they say.
//
//	balance.monitor            A monitor's condition is met by a balance a transaction has just
//	                           moved, so the alert exists only once the condition has been
//	                           evaluated — and THE EVALUATION NOW HAPPENS INSIDE THAT
//	                           TRANSACTION. The pre-write pass covers the single-transaction
//	                           path (prepareBalanceMonitorEvents) and the writer covers every
//	                           other, reading the monitor definitions and applying
//	                           CheckCondition in its own transaction and inserting the canonical
//	                           row there (database.captureBalanceMonitorAlertsInTx). A movement
//	                           made with the alert capture registered — which every process
//	                           built through NewBlnk has — satisfies R-2 with no deferral at
//	                           all, so this member is listed for the population that predates
//	                           that capture rather than for new movements. Those older handoff
//	                           rows still commit BOTH INPUTS TO THE EVALUATION inside the
//	                           balance's transaction, so their verdict was fixed at commit too,
//	                           and BalanceMonitorHandoffProcessor writes the resulting alerts in
//	                           one transaction with the handoff's completion: what was deferred
//	                           was the insert, not the decision, and the handoff cannot be lost
//	                           — only drained late, or recorded failed.
//	bulk_transaction.<status>  A batch summary. Every transaction the batch describes has
//	                           already committed under its own transaction; a bulk request is
//	                           executed one transaction at a time, with compensating void or
//	                           refund as its rollback, so there is no batch-spanning transaction
//	                           at all. The per-transaction events ARE atomic; this one adds the
//	                           summary, which belongs to no single mutation.
//	system.error               An internal-error notification. It describes a fault in the
//	                           process, not a change to the ledger, so it has no mutation of any
//	                           kind. It is the one of the three that does NOT retry its insert;
//	                           see the entry-point list below for why.
//
// THIS IS NOT A LIST OF EXCEPTIONS TO R-2. It is the list of producers whose event row is
// INSERTED after their mutation committed, and for two of the three the decision behind that row
// is committed by the mutation itself. Do not describe any member as exempt from R-2, and do not
// describe this set as though it had a single member — there are three, and exemption is not what
// distinguishes them.
//
// # What these three DO guarantee, and the window that remains
//
// Where a standalone insert IS reached, the two that describe LEDGER STATE spend a bounded,
// retried insert of the SAME prepared row — PublishEventDurably here for balance.monitor, and the
// equivalent loop in sendBulkTransactionWebhook for the batch summary. Retrying the same prepared
// row is what makes the retry idempotent rather than duplicating: an identical stored row is
// adopted as success by the repository instead of being inserted again under a second id.
//
// Once captured, the event is indistinguishable from any other: the same relay, the same bounded
// retry, the same dead-lettering, the same replay, the same metrics.
//
// The window a standalone insert CANNOT close is the one between the mutation's commit and a
// successful insert. The mutation is durable before the first attempt, so no number of attempts
// closes it: a process that dies inside that window loses the event, and there is nothing to
// replay because no row was ever written. It is escalated rather than silent — an exhausted budget
// is logged at error level with the event id and raised through notification.NotifyError, on both
// the monitor and the bulk path.
//
// # Which members that window still applies to, exactly
//
// It is NOT the standing behaviour of all three, and stating it as though it were understates two
// of them:
//
//   - balance.monitor reaches a standalone insert ONLY on a deployment with no broker, where there
//     is no event pipeline to capture into at all. With one configured, the alert row is inserted
//     in the balance's own transaction — evaluated before the write by the pre-write pass, or
//     inside the transaction by the writer for every path that pass does not reach. A handoff row
//     left over from before the writer's capture existed is decided by its mutation and inserted
//     atomically with the handoff's completion, and one whose evaluation budget is spent is
//     RECORDED as failed, so it is countable rather than absent.
//   - bulk_transaction.<status> is inserted in the same transaction as the coordinator's terminal
//     transition. The budget buys that transaction's transient failures; a batch that never
//     finalises stays non-terminal and is countable as an unfinalized batch.
//   - system.error is the one member for which at-most-once is the standing behaviour. It reports
//     a process fault, so there is no ledger state behind it to make durable and no record of its
//     absence to count.
//
// # What closing the residual would take
//
// For system.error it would take a durable record of a fault the process may be unable to write
// at all, which is a contract-level change for whoever owns the plan rather than a decision this
// file may take. For the bulk summary it would take a batch-spanning transaction, which means
// restructuring the transaction-processing pipeline AAP §0.6.2 freezes. Note what closing
// balance.monitor's did NOT take: monitor CONDITION EVALUATION is still frozen domain logic and
// is untouched (AAP §0.6.2, and §0.4.1 scoping the balance.go edit to the transport with "monitor
// condition evaluation is untouched"). model.BalanceMonitor.CheckCondition is the one evaluator,
// called with the same argument by all three routes; what changed is only WHERE it is called from,
// and moving the call inside the transaction is what lets the canonical row go in there too.
//
// docs/event-streaming.md states the same three in subscriber-facing terms. If you change this
// set, change that section in the same commit.
//
// It is a documentation anchor and deliberately carries no behaviour: a const so that a reader
// grepping for the contract lands on prose rather than on one of the three call sites.
const PostCommitEventCaptureContract = "balance.monitor, bulk_transaction.<status>, system.error"

// PublishEventDurably captures a domain event on the standalone path and RETRIES a
// transient persistence failure, for the producers whose mutation is already committed
// by the time the event exists.
//
// # The window this closes, and the one it cannot
//
// PublishEvent makes exactly one insert attempt. That is the right budget for a caller
// that can still fail its own operation — the three entity creations and the single
// transaction path enrol their event in the mutation's transaction, so a failed insert
// rolls the mutation back and nothing is lost. It is the WRONG budget for a caller whose
// mutation has already committed: there the insert is the event's only chance, and a
// momentary connection reset, a brief pool exhaustion or a statement error destroyed the
// event outright, permanently, on the first try. `balance.monitor` was lost that way — the
// balance advanced and its threshold alert simply ceased to exist.
//
// Retrying does not make the capture atomic and this method does not pretend otherwise.
// The mutation is durable before the first attempt, so the window between the commit and
// a successful insert cannot be closed by any number of attempts; a process that dies in
// that window still loses the event. What retrying removes is the far more likely failure:
// a transient database fault.
//
// # This is no longer the primary path for either event that used it
//
// Both events that were captured this way are now written INSIDE a transaction, which is
// what closes the crash window this method cannot:
//
//   - `balance.monitor` is captured in the transaction that moved the balance: by the
//     pre-write pass on the single-transaction path, and by the writer itself on every other,
//     which reads the monitor definitions and applies CheckCondition inside its own
//     transaction. checkBalanceMonitors — the caller below — is reached only on a deployment
//     with NO Kafka broker, where there is no relay and the alert goes straight down the legacy
//     webhook transport. BalanceMonitorHandoffProcessor still converts handoff rows written
//     before the writer's capture existed.
//   - `bulk_transaction.<status>` is captured by finalizeBulkBatchOutcome, in one
//     transaction with the batch coordinator's terminal transition — including for a batch
//     whose start was never recorded, which the repository ADOPTS into that transaction
//     instead of reporting absent. It falls back to this method for one reason only: event
//     capture is unconfigured, so there is no row for a transaction to carry.
//
// So the residual at-most-once behaviour is now confined to narrow, named shapes — a
// broker-less deployment and a finalising transaction that could not commit at all — rather
// than being the standing behaviour of two event families. docs/event-streaming.md states which.
//
// # Why the row is prepared once and the INSERT is what retries
//
// The retry lives beneath PrepareEventOutbox, not above it, and that is load-bearing
// rather than tidy. A `balance.monitor` event id is a fresh UUID by design — a derived id
// would collapse a monitor that fires repeatedly into one event — so re-entering
// PrepareEventOutbox per attempt would mint a NEW id each time. An attempt whose insert
// committed but whose acknowledgement was lost would then be followed by an attempt that
// inserts a SECOND, differently-identified row: one business event delivered twice, with
// nothing at any subscriber able to collapse the pair, because duplicate suppression keys
// on event_id. Retrying the SAME prepared row instead makes the retry idempotent — the
// repository recognises an identical stored row and reports success (see
// resolveDuplicateEventOutboxInsert).
//
// # THE THREE CALLERS OF THIS METHOD, and why system.error is not one of them
//
// TWO SETS OF THREE live in this area and they are NOT the same three. Conflating them is how
// the repository came to carry two different, equally confident claims about which producer the
// third member is. Both sets are stated here, and each is declared in exactly one place.
//
// SET A — the standalone capture SITES, which is what this method serves. Three producers hold
// a mutation that has already committed and no open transaction, so each spends this bounded
// budget on its insert. The set is declared once as postCommitCaptureSites in
// event_producer_atomicity_test.go, which pins each site's budgeted call by source text:
//
//  1. `balance.monitor` — checkBalanceMonitors in balance.go, through this method. Reached only
//     on a deployment with no broker; with one configured the alert row is inserted inside the
//     balance's own transaction — evaluated before the write, or evaluated by the writer in that
//     transaction — and that site returns early.
//  2. `bulk_transaction.<status>` — sendBulkTransactionWebhook in transaction_bulk.go, which
//     spends the same budget through its own equivalent loop so that it can additionally
//     carry batch context in its log fields. The summary itself is ATOMIC — it commits with
//     the coordinator's terminal transition, and a batch whose start was never recorded is
//     ADOPTED into that same transaction rather than captured outside one. What the budget
//     buys is the finalising transaction's own transient failures.
//  3. The status-derived `transaction.*` events of a COALESCED batch — postTransactionActions in
//     transaction_execution.go, through this method. The coalescing writer is driven from
//     transaction_coalescing.go, which AAP §0.6.2 freezes, so its caller cannot thread event
//     rows into it.
//
// SET B — the AT-MOST-ONCE EVENT TYPES, which is what a SUBSCRIBER has to plan for. Declared
// once as PostCommitEventCaptureContract: `balance.monitor`, `bulk_transaction.<status>` and
// `system.error`. docs/event-streaming.md publishes the same three.
//
// The two sets differ on their third member, in both directions, and each difference is a fact
// rather than an inconsistency:
//
//   - `system.error` is in SET B and NOT in SET A. It describes no mutation at all, so there is
//     no ledger state behind it that a retry would protect, and it captures through PublishEvent
//     in a single attempt.
//   - The coalesced batch's `transaction.*` events are in SET A and NOT in SET B. The batch
//     writer now DERIVES those rows from the registered capture and inserts them inside its own
//     transaction (see resolveBatchEventOutboxes), so they are atomic after all. The post-commit
//     copy above still runs, and the derived event id means the repository recognises the
//     identical stored row and reports success, so it is suppressed rather than duplicating.
//
// Every OTHER producer keeps PublishEvent, because its event IS captured inside its
// mutation's transaction and there a failed insert correctly fails the mutation: the three
// entity creations through their repository writers, and the single-transaction and
// rejection paths through theirs.
//
// Parameters:
//   - ctx context.Context: the context for the operation. Cancellation is honoured
//     BETWEEN attempts, so a caller going away stops the retry rather than the sleep.
//   - event NewWebhook: the event name and payload object, unchanged from the producer
//     call site.
//   - options ...EventOption: caller-supplied facts the payload cannot yield, forwarded
//     verbatim to PrepareEventOutbox.
//
// Returns:
//   - error: nil on success and on every no-op; the last persistence error when the
//     retry budget is spent or the failure is not retryable.
func (l *Blnk) PublishEventDurably(ctx context.Context, event NewWebhook, options ...EventOption) error {
	return l.publishEvent(ctx, nil, standaloneEventCaptureAttempts, event, options...)
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
	return l.publishEvent(ctx, tx, singleEventCaptureAttempt, event, options...)
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
// (AAP §0.5.4, "KAFKA_BROKERS empty → legacy path unchanged"), subject to the sunset. The
// whole of it lives in deliverLegacyWebhookOnly, which this delegates to.
//
// The delegate delivers ONLY when tx is nil. An enqueue cannot be rolled back with the
// caller's transaction, so on the in-transaction path this deployment captures nothing and
// delivers nothing, and the caller's post-commit path delivers instead.
//
// # WHICH CAPTURES HAVE NO OPEN TRANSACTION TO JOIN, exhaustively
//
// Requirement R-2 puts the event row inside the same database transaction as the ledger
// mutation that produced it, and PublishEventInTx is how nearly every producer does that.
// The STANDALONE path — tx nil — exists for the captures that have no open transaction to
// join. There are exactly FOUR, and the fourth is the one most easily missed:
//
//  1. balance.monitor. The alert fires because a condition was met on a balance some other
//     transaction has ALREADY committed. Reached only on a broker-less deployment, where the
//     alert is delivered over the legacy transport rather than captured at all; with a broker
//     configured the alert commits inside the balance's own transaction and
//     checkBalanceMonitors returns early.
//  2. The COALESCED transaction batch. Its persistence writer is driven from
//     transaction_coalescing.go, which AAP §0.6.2 freezes, so its caller cannot thread event
//     rows into it. postTransactionActions captures them here instead — durably, and normally
//     redundantly, because the batch writer derives the same rows under its own transaction
//     and the derived event id makes this copy a recognised duplicate rather than a second
//     event.
//  3. The bulk batch SUMMARY, bulk_transaction.<status>. A bulk request executes one
//     transaction at a time with compensating void or refund as its rollback, so by the
//     moment the outcome is known every mutation it describes has already committed under
//     its own transaction: there is no row the summary could be atomic with. The
//     per-transaction events inside the batch ARE atomic — only the summary is not, and only
//     when the batch coordinator row is absent.
//  4. system.error. It describes no mutation of any kind — it reports that something failed —
//     so there has never been a transaction it could have joined. It is the only one of the
//     four that makes a single attempt rather than spending a retry budget.
//
// WHICH OF THOSE CAN ACTUALLY LOSE AN EVENT is a different question with a different answer,
// and PostCommitEventCaptureContract declares that set once. Nothing here may be described as
// the only capture that can lose an event: three of the four can, and each was separately
// documented as the lone case before this was written down.
//
// Everything else — every single-transaction status event, ledger.created, balance.created,
// identity.created — reaches this function with tx non-nil and commits with its mutation.
// A standalone capture of a mutation-derived event outside those four is a defect, not a
// convenience: it converts an exactly-once write into a post-commit window.
//
// # The capture budget applies to the STANDALONE insert only
//
// captureAttempts is how many times the standalone insert of the PREPARED row may be
// attempted. It is singleEventCaptureAttempt for PublishEvent and PublishEventInTx, which
// is why their behaviour is unchanged, and standaloneEventCaptureAttempts for
// PublishEventDurably. It is deliberately ignored on the in-transaction path: PostgreSQL
// marks a transaction ABORTED after any statement error, so a second attempt inside the
// caller's transaction could not succeed, and the correct response there is the rollback
// the returned error produces.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
//   - tx *sql.Tx: the caller's transaction, or nil for the standalone insert.
//   - captureAttempts int: the standalone insert budget. Values below one are treated as
//     one, so a miswired caller still attempts the capture once.
//   - event NewWebhook: the event to capture.
//   - options ...EventOption: caller-supplied facts the payload cannot yield, forwarded
//     verbatim to PrepareEventOutbox.
//
// Returns:
//   - error: nil on success and on every no-op; the persistence error otherwise.
func (l *Blnk) publishEvent(ctx context.Context, tx *sql.Tx, captureAttempts int, event NewWebhook, options ...EventOption) error {
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
	if cnf := l.eventConfiguration(); !eventPublishingConfigured(cnf) && legacyWebhookOnly(cnf) {
		span.AddEvent("Routed to the legacy webhook transport; no Kafka broker is configured")

		// ONE implementation, reached from here. This branch used to be written out inline
		// alongside deliverLegacyWebhookOnly, which had no caller — two copies of a
		// delivery decision, of which only one consulted the sunset. See that function for
		// the in-transaction skip, the nil-queue arm and the sunset gate.
		return l.deliverLegacyWebhookOnly(ctx, tx, event)
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
		}).
			WithField("cause", loggableCause(err)).
			Error("event not captured: publishing is configured but this Blnk instance has no datasource")
		span.RecordError(err)

		return err
	}

	if tx != nil {
		// Inside the caller's ledger transaction: the event commits with the
		// mutation or not at all.
		err = l.datasource.InsertEventOutboxInTx(ctx, tx, outbox)
	} else {
		// No transaction was offered. Three kinds of caller arrive here, and they are
		// not equivalent:
		//
		//   - A POST-COMMIT capture, whose mutation is already durable and whose
		//     insert is therefore the event's only chance: balance.monitor,
		//     bulk_transaction.<status> and a coalesced batch's transaction.* events.
		//     These are the three documented exceptions to requirement R-2; see
		//     PublishEventDurably, which is the budget they use.
		//   - system.error, which describes no mutation at all, so there is nothing it
		//     could have been atomic with.
		//   - ledger.created, identity.created and balance.created ON A DEPLOYMENT WITH
		//     NO PREPARER, i.e. one whose repository writer was given no event row.
		//     Where a preparer exists these are captured inside the creation
		//     transaction and never reach this branch.
		//
		// All of them still belong in the outbox, so they get the same relay retry,
		// dead-letter and replay treatment as every other event once the row is in.
		//
		// The budget is what distinguishes a caller that can still fail its own
		// operation from one whose mutation is already committed; see
		// insertEventOutboxWithRetry and PublishEventDurably.
		err = l.insertEventOutboxWithRetry(ctx, outbox, captureAttempts)
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
		}).
			WithField("cause", loggableCause(err)).
			Error("failed to record event in the outbox")
		span.RecordError(err)

		return err
	}

	span.AddEvent("Event recorded in outbox", trace.WithAttributes(
		attribute.Int64("event.outbox_id", outbox.ID),
	))

	return nil
}

// The capture budgets for the standalone insert.
//
// singleEventCaptureAttempt is the budget every caller had before PublishEventDurably
// existed, and it remains correct for a caller that can still fail its own operation: the
// entity creations and the single transaction path enrol their event in the mutation's
// transaction, so one failed attempt correctly becomes a rollback rather than a retry.
//
// standaloneEventCaptureAttempts is for the caller whose mutation is already durable, where
// the insert is the event's only chance. Three attempts at 200ms then 400ms is DELIBERATELY
// the same budget sendBulkTransactionWebhook already spends on the same kind of work
// (bulkOutcomeCaptureAttempts), so all THREE post-commit captures enumerated on
// PublishEventDurably behave alike rather than each inventing a schedule. It is not trying to be the relay's budget: this is one indexed
// INSERT against the local database, retried to survive a momentary connection reset or a
// brief pool exhaustion. A budget large enough to ride out a real outage would hold a
// post-commit goroutine open for minutes without improving the outcome, because a database
// that is down will still be down — and it would do so while holding a
// postCommitEventPublishSem permit that other events need.
const (
	singleEventCaptureAttempt      = 1
	standaloneEventCaptureAttempts = 3
	standaloneEventCaptureBackoff  = 200 * time.Millisecond
)

// insertEventOutboxWithRetry persists an already-prepared row on the standalone path,
// spending up to attempts tries on a transient failure.
//
// # What it retries, and what it refuses to retry
//
// The repository maps every driver failure onto a typed error before it gets here
// (wrapEventOutboxInsertError), and that classification is what decides:
//
//   - A CONFLICT is not retried. It means the unique index on event_id refused this row,
//     and the repository has already separated the two ways that happens: an identical
//     event already recorded is reported as SUCCESS and never reaches here, so anything
//     that does is a genuine id collision that no further attempt can resolve. Spending
//     the budget on it only delays the error the caller needs to see.
//   - A BAD REQUEST is not retried. A not-null, foreign-key or CHECK violation is a value
//     the schema refuses; re-sending the identical row cannot change that answer.
//   - Everything else is retried. That is the transient class this function exists for, and
//     it is where a connection reset, a pool timeout, a statement timeout and a
//     server-side error all land.
//
// # What is logged, and what is deliberately not
//
// A retry that succeeds is invisible in the outcome, which is exactly when an operator most
// needs to know the database is struggling: a monitor alert that took three tries is a
// signal, not a non-event. So every attempt that will be followed by another is logged, and
// so is a recovery on a later attempt. The fields carry the event identity, the attempt and
// the budget, and the aggregate is HASHED for the same reason it is hashed everywhere else on
// this path — it names whose money the event is about, and this line is emitted on a failure
// path a database problem can make high-volume.
//
// Nothing is logged when the budget is one, which is what keeps every pre-existing caller's
// output unchanged: with no retry to announce there is nothing here to say, and publishEvent
// and the repository each already log the failure with the same event identity. For the same
// reason the terminal failure of a spent budget is not logged here either — a third copy
// would triple one incident in the log.
//
// Parameters:
//   - ctx context.Context: the insert's context. Cancellation is honoured BETWEEN
//     attempts — a caller going away is a reason to stop retrying, not a reason to keep
//     sleeping — and the failure is still returned.
//   - outbox *model.EventOutbox: the PREPARED row. The same row, and therefore the same
//     event_id, is re-sent on every attempt, which is what makes a retry idempotent rather
//     than a second event.
//   - attempts int: the budget. Anything below one is treated as one.
//
// Returns:
//   - error: nil once the row is recorded; otherwise the last failure.
func (l *Blnk) insertEventOutboxWithRetry(ctx context.Context, outbox *model.EventOutbox, attempts int) error {
	if attempts < singleEventCaptureAttempt {
		attempts = singleEventCaptureAttempt
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		lastErr = l.datasource.InsertEventOutbox(ctx, outbox)
		if lastErr == nil {
			if attempt > 1 {
				logrus.WithFields(eventCaptureLogFields(outbox, attempt, attempts)).Warn(
					"the event was recorded in the outbox on a later attempt; an earlier attempt " +
						"failed and the mutation it describes was already committed",
				)
			}

			return nil
		}

		// Attempts REMAIN but will not be spent, which is the only case worth a line of its
		// own: a conflict or a refused value answers identically however many times it is
		// asked, so stopping early is the correct behaviour rather than a shortfall.
		if !standaloneEventCaptureRetryable(lastErr) {
			if attempt < attempts {
				withLoggableCause(
					logrus.WithFields(eventCaptureLogFields(outbox, attempt, attempts)), lastErr).Error(
					"the event could not be recorded in the outbox and the failure is not " +
						"retryable; the remaining attempts are not spent",
				)
			}

			return lastErr
		}

		// The budget is spent. publishEvent's error branch and the repository both log this
		// failure with the same event identity, so it is not logged a third time here.
		if attempt == attempts {
			break
		}

		withLoggableCause(
			logrus.WithFields(eventCaptureLogFields(outbox, attempt, attempts)), lastErr).Warn(
			"failed to record the event in the outbox; retrying",
		)

		select {
		case <-ctx.Done():
			withLoggableCause(
				logrus.WithFields(eventCaptureLogFields(outbox, attempt, attempts)), lastErr).Error(
				"the event was not recorded in the outbox and the context was cancelled before " +
					"the retry budget was spent; the mutation it describes is committed",
			)

			return lastErr
		case <-time.After(standaloneEventCaptureBackoff * time.Duration(attempt)):
		}
	}

	return lastErr
}

// eventCaptureLogFields is the field set every capture-attempt line carries, so the
// attempts of one event join on the same keys.
//
// Parameters:
//   - outbox *model.EventOutbox: the row being captured.
//   - attempt int: the attempt just made, one-based.
//   - attempts int: the budget.
//
// Returns:
//   - logrus.Fields: the structured context for the line.
func eventCaptureLogFields(outbox *model.EventOutbox, attempt, attempts int) logrus.Fields {
	return logrus.Fields{
		"event_id":          outbox.EventID,
		"event_type":        outbox.EventType,
		"topic":             outbox.Topic,
		"aggregate_id_hash": hashLogIdentifier(outbox.AggregateID),
		"attempt":           attempt,
		"max_attempts":      attempts,
		"atomic":            false,
	}
}

// standaloneEventCaptureRetryable reports whether a failed standalone insert is worth
// attempting again.
//
// It reads the TYPED code the repository attached rather than matching on driver strings,
// so it classifies the same way the API layer does and cannot be defeated by a wrapped
// error: isConflictError already covers the id-collision case for the dead-letter path, and
// the bad-request case is read through apierror.Normalize so the legacy and canonical codes
// are the same answer.
//
// An error carrying NO api code is treated as retryable. That is the conservative
// direction: an unclassified failure is most often a connection- or driver-level fault,
// which is precisely the transient class, and the cost of being wrong is two wasted
// attempts against the cost of losing an event.
//
// Parameters:
//   - err error: the insert failure. nil answers false, because there is nothing to retry.
//
// Returns:
//   - bool: true when another attempt could plausibly succeed.
func standaloneEventCaptureRetryable(err error) bool {
	if err == nil {
		return false
	}

	// The unique index refused this event id, and an identical stored row would have been
	// reported as success. No further attempt can resolve a genuine collision.
	if isConflictError(err) {
		return false
	}

	// A value the schema or the validator refuses. Re-sending the identical row cannot
	// change the answer.
	return !eventCaptureBadRequest(err)
}

// eventCaptureBadRequest reports whether an error carries the generic bad-request code.
//
// It mirrors isConflictError and isInternalServerError: the code is read through
// apierror.Normalize so the legacy BAD_REQUEST the database layer still constructs and the
// canonical GEN_BAD_REQUEST classify identically, and errors.As is used rather than a type
// assertion so a wrapped error classifies the same as a bare one.
//
// Parameters:
//   - err error: the error to classify. May be nil.
//
// Returns:
//   - bool: true when the error is a bad request.
func eventCaptureBadRequest(err error) bool {
	if err == nil {
		return false
	}

	isBadRequestCode := func(code apierror.ErrorCode) bool {
		return apierror.Normalize(code) == apierror.ErrGenBadRequest
	}

	var apiErr apierror.APIError
	if errors.As(err, &apiErr) {
		return isBadRequestCode(apiErr.Code)
	}

	var apiErrPtr *apierror.APIError
	if errors.As(err, &apiErrPtr) && apiErrPtr != nil {
		return isBadRequestCode(apiErrPtr.Code)
	}

	return false
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
// # THE SUNSET IS CONSULTED HERE, through the same helper every other consumer uses
//
// This is a legacy HTTP enqueue, so it is subject to requirement R-12's retirement exactly
// as the relay's dual-delivery leg is. It previously was not: the branch compared nothing
// against the sunset, so a deployment with a webhook URL, no brokers and a sunset date long
// past kept delivering over a transport that was supposed to be gone — while the relay and
// the 410 guard on a Kafka deployment both treated it as retired. Two callers of one
// decision, one of which did not ask.
//
// The predicate is blnk.WebhookSunsetPassedNow, which reads event_sunset.go's single
// comparison. It answers false — legacy continues — when no window is configured AND no
// Kafka transport exists, which is the ordinary state of a webhook-only deployment that has
// not scheduled a retirement; it answers true once a configured, parseable sunset instant has
// arrived. So nothing changes for a deployment that never opted into the migration, and the
// date is honoured for one that did.
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
//   - error: whatever SendWebhook reports, or nil on the in-transaction skip and on the
//     post-sunset skip.
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

	// RETIRED MEANS RETIRED, on every transport-selecting path rather than only on the
	// relay's. Skipping is not an error: the event is not lost, it is simply no longer owed
	// to a transport that no longer exists, and after the sunset a webhook-only deployment
	// has been told for thirty days that this is what happens.
	if WebhookSunsetPassedNow() {
		span.AddEvent("Legacy-only delivery skipped: the webhook sunset has passed")
		logrus.WithFields(logrus.Fields{
			"event_type":  event.Event,
			"legacy_only": true,
		}).Warn(
			"the legacy HTTP webhook transport is retired — the configured webhook deprecation " +
				"sunset has passed — and no Kafka broker is configured, so this event was NOT " +
				"delivered anywhere. Configure KAFKA_BROKERS to publish it",
		)

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
		}).
			WithField("cause", loggableCause(err)).
			Error("event not delivered: the legacy webhook transport has no queue client")
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
		}).
			WithField("cause", loggableCause(err)).
			Error("failed to enqueue the legacy webhook delivery")
		span.RecordError(err)

		return err
	}

	span.AddEvent("Event delivered over the legacy webhook transport")

	return nil
}
