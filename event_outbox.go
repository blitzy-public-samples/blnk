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

// event_outbox.go is the producer-facing edge of the Kafka event-publishing pipeline.
// It does exactly two things, and deliberately nothing else:
//
//  1. PrepareEventOutbox turns a domain event into a blnk.event_outbox row. It is
//     modelled on PrepareLineageOutbox, which is the house pattern for outbox row
//     construction in this repository, and it is where THE PAYLOAD-PRESERVATION
//     GUARANTEE is implemented rather than merely asserted.
//  2. PublishEvent (and its in-transaction sibling PublishEventInTx) persists that
//     row. It is the one-for-one transport substitution for SendWebhook at every
//     producer call site.
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

// NewWebhook is the webhook notification envelope, and it is a FROZEN CONTRACT. It
// includes an event type and associated payload data.
type NewWebhook struct {
	Event   string      `json:"event"` // The event type that triggered the webhook.
	Payload interface{} `json:"data"`  // The data associated with the event.
}

// defaultEventMaxAttempts is the retry budget stamped on an outbox row when the
// configured relay attempt limit is unavailable or nonsensical.
const defaultEventMaxAttempts = 5

// THE UNKEYED SENTINEL IS GONE, and its removal is the point of this note.

// postCommitEventPublishSem bounds how many post-commit event captures may be in flight
// at once, across every transaction and balance in the process.
//
// The balance-monitor fan-out is the worst shape of it. One balance can carry many
// monitors and many of them can fire on one update, so the number of goroutines is a
// PRODUCT of two counts rather than one per transaction.
var postCommitEventPublishSem = make(chan struct{}, 64)

// eventConfiguration reads the configuration this file needs, tolerating both an
// uninitialised Blnk instance and an unloaded configuration store.
//
// Returns:
//   - *config.Configuration: the effective configuration, or nil when none can be
//     resolved.
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
// That reasoning is only sound while a relay is running, and the relay REFUSES TO RUN
// without Kafka: it would otherwise be handed the no-op publisher, report every publish
// as dispatched and retire the whole outbox having sent nothing.
//
// Parameters:
//   - cnf *config.Configuration: the configuration to inspect. May be nil.
//
// Returns:
//   - bool: true when at least one usable Kafka broker is configured.
func eventPublishingConfigured(cnf *config.Configuration) bool {
	// Delegated rather than reimplemented. The database layer asks the SAME question when
	// it decides whether to write a balance-monitor handoff inside a ledger transaction,
	// and the two answers must be identical: a disagreement would either write handoffs
	// nothing drains, or leave a movement whose monitors neither the handoff nor the
	// post-commit path evaluates. One implementation makes that unrepresentable.
	return cnf.EventPublishingConfigured()
}

// legacyWebhookOnly reports that this deployment has a webhook URL and no Kafka broker,
// which is the pre-migration steady state and the state every deployment is in before
// it opts in.
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
// Returns:
//   - bool: true when Kafka brokers or a legacy webhook URL are configured.
func (l *Blnk) eventCaptureEnabled() bool {
	if l == nil {
		return false
	}

	return eventPublishingConfigured(l.eventConfiguration())
}

// publishEntityEventWhenUncaptured delivers a ledger, identity or balance creation
// event from the post-commit path WHEN, AND ONLY WHEN, the repository did not capture
// it.
//
// The preparer constructors return nil when eventCaptureEnabled is false, so that a
// deployment with no broker does not pay a transaction per creation to insert no event.
//
// Parameters:
//   - ctx context.Context: the publishing context. Callers detach it from request
//     cancellation before handing it over; see postBalanceActions.
//   - aggregateID string: the created entity's identifier. Empty means nothing was
//     created.
//   - event NewWebhook: the event string and payload object, unchanged from what the
//     legacy transport was handed.
//   - options ...EventOption: applied only on the outbox path, which this fallback
//     cannot reach; passed so the two capture sites read identically.
//
// Returns:
//   - error: SendWebhook's enqueue error, exactly as the pre-migration call site
//     returned it, or nil when there was nothing to do.
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
// that an operator can extend the budget for one stuck event without reconfiguring and
// restarting the relay, and so that a configuration change never retroactively alters
// the budget of rows already in flight.
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

// mapStringValue extracts a trimmed string value from a map payload, tolerating a value
// that is not a string.
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

// eventAggregateID derives the aggregate identifier — the entity the event is ABOUT —
// from the payload object the producer handed over.
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
		// Bulk transaction events. batch_id is the aggregate: one batch produces a sequence
		// of bulk_transaction.<status> events describing its progress, and grouping them by
		// batch is what makes that sequence readable.
		return mapStringValue(typed, "batch_id")
	default:
		// Any other payload shape, system.error's {"error", "time"} map included (it is a
		// map[string]interface{} with no batch_id, so it arrives here via the map arm
		// returning ""). There is no aggregate to name; the caller falls back.
		return ""
	}
}

// resolveEventPartitionKey picks the Kafka message key and reports WHICH dimension it
// came from.
//
// Precedence, and why it is this order:
//
//  1. derived — the key eventPartitionKey resolved from the payload. That function
//     applies the ledger rule itself: a payload that knows its ledger returns the
//     ledger, whatever its category, so a non-empty answer here is the ledger for every
//     ledger-bearing event and the event's own aggregate otherwise.
//  2. aggregate — the aggregate id resolved separately by eventAggregateID, for a
//     payload shape eventPartitionKey has no arm for but that still names something.
//  3. eventType — the type itself, which is what gives system.error a single partition
//     and therefore a total order.
//
// The dimension reported for candidates 1 and 2 is ledger when the caller's derived key
// IS the ledger and aggregate otherwise, which the caller decides by passing the ledger
// it resolved; this function only distinguishes "from the payload" from "from the type".
// The ledger-versus-aggregate question is answered by eventLedgerID, and duplicating it
// here would create a second answer able to disagree.
//
// Parameters:
//   - derived string: the key resolved from the payload, or the caller-supplied ledger
//     when one was given.
//   - aggregateID string: the separately resolved aggregate id. Empty when there is
//     none.
//   - eventType string: the trimmed event name. Empty only for a zero-valued event.
//   - ledgerID string: the authoritative ledger, empty when the event has none.
//
// Returns:
//   - string: the chosen key, empty when every candidate is empty.
//   - model.EventKeyDimension: the dimension the key came from. Meaningless when the
//     key is empty, and the caller refuses that case.
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
// That ledger arrives through WithEventLedgerID, supplied by the producer, and it
// OVERRIDES whatever this function returns. Every ledger-scoped producer supplies it:
// transaction execution (resolved from the loaded source balance, falling back to the
// destination), the balance post-action and monitor check, and the ledger post-action.
//
// Parameters:
//   - payload interface{}: the NewWebhook payload object. May be nil or of any type.
//
// Returns:
//   - string: the partition key, or "" when the payload carries none.
func eventPartitionKey(payload interface{}) string {
	// applied before any per-type fallback: an event that knows its ledger is keyed by its
	// ledger, whatever its category.
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
		// No fallback to BalanceID here, unlike the partition key.
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
// Parameters:
//   - source, destination, transactionID string: the candidate keys, in preference
//     order.
//
// Returns:
//   - string: the first non-blank candidate, or "" when all three are blank.
func transactionPartitionKey(source, destination, transactionID string) string {
	return firstNonBlank(source, destination, transactionID)
}

// firstNonBlank returns the first candidate that is not empty once trimmed, or "" when
// every candidate is blank.
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

// PrepareEventOutbox builds the blnk.event_outbox row for a domain event, ready to be
// inserted either standalone or inside a caller's ledger transaction.
//
// A payload that CANNOT BE MARSHALED is an ERROR, and this is a deliberate change from
// the earlier behaviour of logging it and returning nil.
//
// EventOption supplies a fact about an event that the payload cannot yield.
//
// Parameters:
//   - ctx context.Context: the context for the operation, used for tracing only.
//   - event NewWebhook: the event name and payload object, passed through unchanged
//     from the producer call site.
//   - options ...EventOption: facts the payload cannot yield.
//
// Returns:
//   - *model.EventOutbox: the row to persist, or nil when publishing is unconfigured.
//   - error: nil on success and on the unconfigured no-op; a typed internal-server
//     error when the payload cannot be marshaled.
type EventOption func(*eventAttributes)

// eventAttributes carries what a caller supplied, before any derivation runs.
//
// It is a struct rather than a bare string so that adding a second caller-supplied
// attribute is an additive change here instead of a new parameter everywhere.
type eventAttributes struct {
	// ledgerID is the ledger the mutation belonged to, as supplied by the caller.
	// Empty means "not supplied", which sends the derivation on to the payload.
	ledgerID string
	// identity is a caller-supplied stable identity for the event, used to DERIVE its id.
	// Empty means "not supplied", which sends the decision on to model.EventIdentityFor.
	// See WithEventIdentity.
	identity string
}

// WithEventLedgerID supplies THE LEDGER THE MUTATION BELONGED TO.
//
// model.Transaction HAS NO LEDGER FIELD. A transaction's ledger association is
// indirect, through the balances it moves value between, so a transaction payload
// simply cannot yield the ledger — and MetaData is caller-controlled and unfit to
// derive a routing key from.
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
// A supplied identity wins over model.EventIdentityFor, including over its refusal to
// derive. That is the whole purpose: the caller is asserting an identity the payload
// could not express.
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
// A nil option is skipped rather than panicking: the list is variadic and assembled at
// call sites that may build it conditionally, and a nil entry there is a caller's slip
// that must not take a ledger write down with it.
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

	// THE PAYLOAD GUARANTEE: marshal the whole NewWebhook value, both keys, exactly as
	// SendWebhook does. Never the inner payload alone, never re-shaped, never re-keyed.
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

	// TWO COLUMNS, ONE RULE. They are resolved by two functions and stored in two columns
	// because they diverge on the FALLBACK: an event with no ledger still needs a stable
	// key, while its ledger column must stay NULL rather than hold a balance, identity or
	// batch id.
	partitionKey := eventPartitionKey(event.Payload)
	ledgerID := eventLedgerID(event.Payload)

	// A CALLER-SUPPLIED LEDGER WINS over both derivations, and over each for its own
	// reason. It is the authoritative ledger, so it is what ledger_id must record — a
	// payload that yields none (a transaction) would otherwise store SQL NULL.
	attributes := applyEventOptions(options)
	if supplied := attributes.ledgerID; supplied != "" {
		ledgerID = supplied
		partitionKey = supplied
	}

	// THE KEY IS RESOLVED AGAINST A DECLARED DIMENSION, not through an untyped chain.
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
		// transaction queue already shards on — keeps it ordered against that balance's other
		// events, which is the strongest guarantee available for it.
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
		// Resolved once, at construction, and stored on the row. The relay never re-derives
		// it, so a row stays bound to its ORIGINAL destination even if KAFKA_TOPIC_PREFIX
		// changes afterwards.
		Topic:         TopicForEvent(eventType),
		SchemaVersion: model.SchemaVersionV1,
		Payload:       payloadBytes,
		// UTC so the RFC3339 rendering on the wire is unambiguous and identical wherever the
		// process runs. time.Time's standard JSON encoding is already RFC3339, so no custom
		// marshaller is involved.
		OccurredAt: time.Now().UTC(),
		// Set explicitly even though both the repository layer and the column default would
		// supply it, so the in-memory row the caller holds agrees with the row that lands in
		// the table.
		Status:      model.EventOutboxStatusPending,
		MaxAttempts: eventMaxAttempts(cnf),
	}

	// THE CANONICAL EVENT VALUE IS PRODUCED ONCE, HERE, and everything downstream reuses
	// these bytes: the Kafka publish, the dead-letter copy (which splices failure metadata
	// onto them) and a dead-letter replay. Nothing re-marshals the envelope.
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
	outbox.Traceparent, outbox.Tracestate = captureTraceContext(ctx)

	// AN UNCATALOGUED EVENT TYPE IS A DEFECT SIGNAL, and this is where it is raised.
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
// THIS FUNCTION DOES NOT TALK TO KAFKA. It writes one pending row and returns.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
//   - event NewWebhook: the event name and payload object, unchanged from the producer
//     call site.
//
// Returns:
//   - error: nil on success and on every no-op; the persistence error otherwise.
func (l *Blnk) PublishEvent(ctx context.Context, event NewWebhook, options ...EventOption) error {
	return l.publishEvent(ctx, nil, singleEventCaptureAttempt, event, options...)
}

// PostCommitEventCaptureContract is the SINGLE place the post-commit producers are
// described, and it exists because the alternative was three files each claiming to
// hold the only one.
//
// Once captured, the event is indistinguishable from any other: the same relay, the
// same bounded retry, the same dead-lettering, the same replay, the same metrics.
const PostCommitEventCaptureContract = "balance.monitor, bulk_transaction.<status>, system.error"

// PublishEventDurably captures a domain event on the standalone path and RETRIES a
// transient persistence failure, for the producers whose mutation is already committed
// by the time the event exists.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
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

// PublishEventInTx captures a domain event inside an existing database transaction, so
// the event and the ledger mutation that produced it commit or roll back together.
//
// There is no window in which a balance moved but its event was lost, and none in which
// an event describes a mutation that was rolled back. That is what makes exactly-once
// semantics on the write side possible, layered over Kafka's at-least-once delivery.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
//   - tx *sql.Tx: the caller's open transaction. May be nil, which selects the
//     standalone insert.
//   - event NewWebhook: the event name and payload object, unchanged from the producer
//     call site.
//
// Returns:
//   - error: nil on success and on every no-op; the persistence error otherwise.
func (l *Blnk) PublishEventInTx(ctx context.Context, tx *sql.Tx, event NewWebhook, options ...EventOption) error {
	return l.publishEvent(ctx, tx, singleEventCaptureAttempt, event, options...)
}

// publishEvent is the single implementation behind PublishEvent and PublishEventInTx.
//
// Both entry points share one body so the two paths cannot diverge: they build the row
// identically and differ only in which repository method persists it. A second copy of
// the guard sequence would be a second place for the no-op contract to rot.
//
// Parameters:
//   - ctx context.Context: the context for the operation.
//   - tx *sql.Tx: the caller's transaction, or nil for the standalone insert.
//   - captureAttempts int: the standalone insert budget. Values below one are treated
//     as one, so a miswired caller still attempts the capture once.
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
	if cnf := l.eventConfiguration(); !eventPublishingConfigured(cnf) && legacyWebhookOnly(cnf) {
		span.AddEvent("Routed to the legacy webhook transport; no Kafka broker is configured")

		// ONE implementation, reached from here. See that function for the in-transaction
		// skip, the nil-queue arm and the sunset gate.
		return l.deliverLegacyWebhookOnly(ctx, tx, event)
	}

	outbox, err := l.PrepareEventOutbox(ctx, event, options...)
	if err != nil {
		// A payload that will not serialise is a defect, not a transient condition, and it is
		// returned rather than swallowed so the caller — and, on the in-transaction path, the
		// caller's rollback — can act on it.
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

	// Checked AFTER the row is built, deliberately. Preparing the row costs one marshal
	// and no I/O, and it is what gives this failure the event identity that makes it
	// actionable — "some event was dropped" is not a diagnosable message. The nil-receiver
	// arm is evaluated first so the field access can never run on a nil pointer.
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
		// No transaction was offered. Three kinds of caller arrive here, and they are not
		// equivalent:
		err = l.insertEventOutboxWithRetry(ctx, outbox, captureAttempts)
	}
	if err != nil {
		// Logged here for immediate operator visibility, and returned so the caller's
		// existing error handling — which routes to notification.NotifyError at most call
		// sites — behaves exactly as it did with SendWebhook.
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
const (
	singleEventCaptureAttempt      = 1
	standaloneEventCaptureAttempts = 3
	standaloneEventCaptureBackoff  = 200 * time.Millisecond
)

// insertEventOutboxWithRetry persists an already-prepared row on the standalone path,
// spending up to attempts tries on a transient failure.
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
// Parameters:
//   - ctx context.Context: the insert's context.
//   - outbox *model.EventOutbox: the PREPARED row.
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
// Parameters:
//   - err error: the insert failure. nil answers false, because there is nothing to
//     retry.
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
// apierror.Normalize so the legacy BAD_REQUEST the database layer still constructs and
// the canonical GEN_BAD_REQUEST classify identically, and errors.As is used rather than
// a type assertion so a wrapped error classifies the same as a bare one.
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
// Two callers of one decision, one of which did not ask.
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
	// relay's. Skipping is not an error: the event is not lost, it is simply no longer
	// owed to a transport that no longer exists, and after the sunset a webhook-only
	// deployment has been told for thirty days that this is what happens.
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
	// always builds it from the Redis DSN, so this is unreachable in a real deployment; it
	// is reachable from a hand-assembled instance, and a panic inside a post-action
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
		// Logged and returned, exactly as the outbox branch does, so a call site's existing
		// routing to notification.NotifyError behaves identically whichever transport is in
		// use.
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
