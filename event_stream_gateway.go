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
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

// ---------------------------------------------------------------------------------------
// The subscriber stream gateway — the enforcement point for a partition-key scope
//
// # Why this exists
//
// Requirement R-7 gives a subscriber an access model with three dimensions: the topics it
// may read, the consumer-group namespace it owns, and a partition-key prefix confining it to
// the records it is entitled to. Kafka's authorizer enforces the first two. It cannot
// enforce the third at all — its resource types are Topic, Group, Cluster, TransactionalId
// and DelegationToken, and none of them is a message key — and a topic per key space is
// excluded by the frozen shared-topic design.
//
// Two answers were tried before this one and both were wrong. Refusing to issue a credential
// to a key-scoped subscriber withdrew a mandatory capability permanently. Issuing whole-topic
// Read and DECLARING that applying the prefix was the subscriber's own obligation was
// truthful prose about an absent boundary: a subscriber that ignored it, or simply used a
// different Kafka client, read every record on the shared category topic — including records
// written for other ledgers and other subscribers — and nothing in the platform could
// prevent or detect it. Cooperation is not an access boundary.
//
// So the boundary is a component. A key-scoped subscriber is provisioned with Describe and
// NO Read on its topics, which closes the broker path outright, and this gateway is the path
// that remains: it authenticates the subscriber with its own SASL credential, authorises the
// topic against the registry, reads with Blnk's credential, and applies
// model.EventSubscriber.HasKeyAccess to EVERY record before any of them leaves the process.
//
// # What it is not
//
// It is not a consumer library, and it does not manage subscriber-side dead-lettering — both
// are explicitly out of scope. It holds no consumer group, commits no offsets and keeps no
// per-subscriber state: the cursor is the caller's, carried in the request and returned in
// the response, which is what keeps this a stateless read that any replica can serve.
// ---------------------------------------------------------------------------------------

const (
	// DefaultSubscriberStreamLimit is the page size a request that names none receives.
	//
	// A hundred records is large enough that a caught-up subscriber makes one round trip per
	// poll rather than several, and small enough that the JSON body stays in the hundreds of
	// kilobytes for Blnk's own event sizes.
	DefaultSubscriberStreamLimit = 100

	// MaxSubscriberStreamLimit is the ceiling on a requested page size.
	//
	// A ceiling rather than a refusal, because a caller asking for more than this is asking
	// for a throughput property rather than making an error, and clamping serves it. The
	// response reports what was actually read, so a client cannot mistake the clamp for an
	// empty tail.
	MaxSubscriberStreamLimit = 500

	// MaxSubscriberStreamBytes bounds the response the broker is asked to build.
	//
	// It is the bound that actually protects this process: a record count cannot, because one
	// oversized record exceeds any count. Five mebibytes is comfortably above Blnk's own
	// event sizes at the maximum page size and well below the default broker message ceiling.
	MaxSubscriberStreamBytes int64 = 5 << 20

	// DefaultSubscriberStreamMaxWait is how long the broker may hold a request waiting for
	// records when the caller names no preference.
	//
	// One second makes a caught-up subscriber's poll cheap without turning it into a busy
	// loop, and it is a CEILING: MinBytes is one byte, so the broker answers the moment
	// anything is available.
	DefaultSubscriberStreamMaxWait = time.Second

	// MaxSubscriberStreamMaxWait is the ceiling on a requested wait.
	//
	// It bounds how long one HTTP request may occupy a connection doing nothing, which is
	// what stops a client from converting long-polling into a connection-exhaustion tool.
	MaxSubscriberStreamMaxWait = 10 * time.Second

	// SubscriberStreamBudget is the total deadline one stream read may consume.
	//
	// It exceeds MaxSubscriberStreamMaxWait by enough to cover the registry read, the SASL
	// handshake on a cold transport and the fetch itself, so a request that waits the full
	// ten seconds for records still answers rather than expiring on its own ceiling.
	SubscriberStreamBudget = 20 * time.Second

	// SubscriberStreamOffsetEarliest asks for the earliest record still retained.
	//
	// It is Kafka's own sentinel value rather than a Blnk invention, so the number a client
	// sends is the number the protocol defines.
	SubscriberStreamOffsetEarliest = int64(kafka.FirstOffset)

	// SubscriberStreamOffsetLatest asks for the end of the log, which returns nothing now and
	// gives a caller the offset to resume from.
	SubscriberStreamOffsetLatest = int64(kafka.LastOffset)
)

var (
	// ErrSubscriberStreamUnauthenticated is the gateway refusing a caller whose presented
	// credential does not match the one the registry recorded.
	//
	// IT IS DELIBERATELY THE SAME ERROR FOR EVERY CAUSE: no such subscriber, a subscriber
	// holding no credential, the wrong principal, the wrong secret, and a credential whose
	// revocation is pending. Distinguishing them would let an unauthenticated caller
	// enumerate the registry one 404 at a time, and the caller can do nothing differently
	// for any of them.
	ErrSubscriberStreamUnauthenticated = errors.New("blnk: the presented subscriber credential is not valid")

	// ErrSubscriberStreamTopicNotGranted is the gateway refusing an AUTHENTICATED subscriber
	// that asked for a topic outside its authorized_topics, or for a topic no subscriber may
	// be granted at all — a dead-letter topic or Blnk's internal system category.
	//
	// Naming the topic in the message is safe here and useful: the caller sent it, and it
	// authenticated as a subscriber whose grant it can already read through GET /subscribers.
	ErrSubscriberStreamTopicNotGranted = errors.New("blnk: the subscriber is not granted this topic")

	// ErrSubscriberStreamUnavailable is the gateway with no broker to read from.
	//
	// A deployment with no KAFKA_BROKERS publishes nothing, so there is nothing to stream;
	// this is the same honest 503 the credential endpoint answers with rather than an empty
	// page, which would read as "caught up".
	ErrSubscriberStreamUnavailable = errors.New("blnk: no Kafka broker is configured, so there is no event stream to read")
)

// subscriberStreamRegistry is the ONE registry read the gateway performs.
//
// A one-method seam rather than the subscriber service's full store, because the gateway is
// on the data plane and must not be able to write: no credential record, no fence, no
// authorization change. What it cannot reach it cannot break.
type subscriberStreamRegistry interface {
	// GetEventSubscriberByID reads one subscriber by its business key, returning a typed
	// not-found error rather than a bare sql.ErrNoRows.
	GetEventSubscriberByID(ctx context.Context, subscriberID string) (*model.EventSubscriber, error)
}

// subscriberRecordFetcher is the ONE broker read the gateway performs.
//
// It is satisfied by *KafkaAdminClient. Kept as a seam so the gateway's authorization and
// filtering can be tested exhaustively with no broker — which is where the assertions that
// matter live, because "a record outside the scope is never returned" is a property of this
// code and not of Kafka.
type subscriberRecordFetcher interface {
	// FetchTopicRecords reads a bounded page of records from one partition.
	FetchTopicRecords(ctx context.Context, req TopicRecordFetch) (TopicRecordBatch, error)

	// IsConfigured reports whether any broker is configured, so the gateway can answer an
	// unconfigured deployment without a round trip.
	IsConfigured() bool
}

// SubscriberStreamRequest is one subscriber asking for the next page of its own events.
type SubscriberStreamRequest struct {
	// SubscriberID identifies the registry row whose grant and key scope are applied.
	SubscriberID string

	// Principal is the SASL username the caller presented, and it is OPTIONAL: the
	// registry already knows which principal belongs to this subscriber, so a caller may
	// omit it. When it IS sent it must match, because a caller sending a different
	// principal is asking about a different identity and answering it would be answering a
	// question it did not ask.
	Principal string

	// Secret is the SASL password the caller presented. It is compared against the stored
	// credential reference in constant time and is never logged, stored or echoed.
	Secret string

	// Topic is the fully-qualified topic to read.
	Topic string

	// Partition is the partition to read. Zero is a legitimate partition, so there is no
	// "unset" value here and none is needed: a caller reading one partition at a time is
	// the only shape a resumable cursor has.
	Partition int

	// Offset is where to resume. Absolute at or above zero; the two Kafka sentinels are
	// honoured through SubscriberStreamOffsetEarliest and SubscriberStreamOffsetLatest.
	Offset int64

	// Limit is the requested page size. Non-positive selects the default and an oversized
	// value is clamped.
	Limit int

	// MaxWait is how long the broker may hold the read waiting for records. Non-positive
	// selects the default and an oversized value is clamped.
	MaxWait time.Duration
}

// SubscriberStreamRecord is one record the subscriber is entitled to.
type SubscriberStreamRecord struct {
	// Offset is the record's position in the partition.
	Offset int64

	// Partition echoes which partition it came from, so a record remains self-describing
	// once a client has merged pages from several partitions.
	Partition int

	// Key is the message key as a string. For Blnk's own events it is the partition key —
	// the ledger id for a ledger-scoped event — and it is returned because it is the value
	// the subscriber's own prefix was matched against, so a client can verify the boundary
	// it was promised.
	Key string

	// Timestamp is the record's broker timestamp.
	Timestamp time.Time

	// Value is the record's body verbatim: the marshalled LedgerEvent envelope, byte for
	// byte as it sits on the topic. It is not re-marshalled, so a subscriber reading
	// through the gateway and a subscriber reading a topic directly parse identical bytes.
	Value []byte
}

// SubscriberStreamPage is the answer to one stream read.
type SubscriberStreamPage struct {
	// SubscriberID, Topic and Partition echo what was read.
	SubscriberID string
	Topic        string
	Partition    int

	// Records are the records this subscriber is entitled to, in offset order.
	Records []SubscriberStreamRecord

	// NextOffset is where the caller resumes. It advances past WITHHELD records too, which
	// is the property that stops a key-scoped subscriber being stalled forever by a run of
	// records belonging to someone else.
	NextOffset int64

	// HighWatermark is the offset the next record produced will occupy, so a client can
	// compute its own lag without a second endpoint.
	HighWatermark int64

	// LogStartOffset is the earliest offset still retained, which is what tells a client
	// its cursor has fallen off the back of the log rather than merely being behind.
	LogStartOffset int64

	// RecordsScanned is how many records the gateway read before filtering, and
	// RecordsWithheld how many of them the key scope excluded. Both are reported because
	// they are how a subscriber and an operator can see the boundary working: a page with
	// zero records and a non-zero withheld count is a working filter, not a broken feed.
	RecordsScanned  int
	RecordsWithheld int

	// Truncated reports that the page size cut the read short, so more records are
	// immediately available at NextOffset.
	Truncated bool

	// KeyScope is the prefix that was applied, and KeyScopeEnforced states that it was.
	// They travel with the records so that the boundary is stated in the same body it was
	// applied to.
	KeyScope         string
	KeyScopeEnforced bool
}

// SubscriberStreamGateway serves a subscriber's own events over HTTP, applying its key scope.
//
// It is constructed per request from process-scoped dependencies and holds no state of its
// own, so nothing here needs closing and nothing is shared between callers.
type SubscriberStreamGateway struct {
	// registry is the read that resolves a subscriber's credential reference, topic grant
	// and key scope. All three come from the row, so a stale in-memory copy cannot widen
	// access.
	registry subscriberStreamRegistry

	// fetcher is the broker read. Nil means the deployment has no Kafka, which is answered
	// as unavailable rather than as an empty page.
	fetcher subscriberRecordFetcher
}

// NewSubscriberStreamGateway builds a gateway over a registry read and a broker read.
//
// Parameters:
//   - registry subscriberStreamRegistry: the subscriber lookup. Nil yields a gateway that
//     reports an unavailable registry rather than panicking.
//   - fetcher subscriberRecordFetcher: the record read. Nil yields a gateway that reports
//     the stream unavailable, which is the correct answer for a deployment with no broker.
//
// Returns:
//   - *SubscriberStreamGateway: the gateway.
func NewSubscriberStreamGateway(
	registry subscriberStreamRegistry,
	fetcher subscriberRecordFetcher,
) *SubscriberStreamGateway {
	return &SubscriberStreamGateway{registry: registry, fetcher: fetcher}
}

// ReadEvents authenticates a subscriber, authorises the topic, and returns the next page of
// records its key scope admits.
//
// # The order of the checks is the security property
//
// Authentication precedes authorization, and both precede the broker read. A gateway that
// fetched first and filtered afterwards would have read records into this process on behalf
// of a caller that had proven nothing — and a partial failure after the read is exactly how
// records reach a log line or an error body they do not belong in.
//
// # Why every refusal is typed
//
// The API layer maps these to 401, 403 and 503 respectively. Without typed errors an
// authentication failure would resolve to the unknown-code 500 default, which a client
// retries — turning a rejected secret into a retry loop against a decision that will not
// change.
//
// Parameters:
//   - ctx context.Context: bounds the registry read and the fetch. The caller sets the
//     budget; SubscriberStreamBudget is the intended ceiling.
//   - req SubscriberStreamRequest: what to read and the credential to read it with.
//
// Returns:
//   - SubscriberStreamPage: the records the subscriber is entitled to, plus its cursor and
//     the partition's watermarks.
//   - error: a typed apierror for every refusal — invalid credential, topic not granted,
//     malformed request, unavailable stream — or a wrapped broker failure.
func (g *SubscriberStreamGateway) ReadEvents(
	ctx context.Context,
	req SubscriberStreamRequest,
) (SubscriberStreamPage, error) {
	page := SubscriberStreamPage{}

	subscriber, err := g.authenticate(ctx, req)
	if err != nil {
		return page, err
	}

	topic, err := g.authorizeTopic(subscriber, req.Topic)
	if err != nil {
		return page, err
	}

	fetch, err := g.boundedFetch(req, topic)
	if err != nil {
		return page, err
	}

	batch, err := g.fetcher.FetchTopicRecords(ctx, fetch)
	if err != nil {
		return page, streamFetchFailure(subscriber, topic, err)
	}

	return g.filter(subscriber, topic, fetch, batch), nil
}

// authenticate resolves the row and proves the caller holds its credential.
//
// # Why a not-found subscriber is an authentication failure and not a 404
//
// This is the only endpoint in the subscriber surface a NON-OPERATOR reaches, and a 404 here
// would answer "does subscriber X exist?" to anyone who asks. Subscriber identifiers are
// caller-chosen names, so that is an enumeration oracle over tenant names. Every failure
// therefore returns one indistinguishable refusal, and the reason is recorded in a log line
// instead — where an operator can read it and a caller cannot.
//
// Parameters:
//   - ctx context.Context: bounds the registry read.
//   - req SubscriberStreamRequest: the identifier and the presented credential.
//
// Returns:
//   - *model.EventSubscriber: the authenticated row, never nil on success.
//   - error: ErrSubscriberCredentialInvalid for every authentication failure, or a
//     validation error for a malformed request.
func (g *SubscriberStreamGateway) authenticate(
	ctx context.Context,
	req SubscriberStreamRequest,
) (*model.EventSubscriber, error) {
	if g == nil || g.registry == nil {
		return nil, apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"The subscriber event stream is unavailable because this process has no subscriber registry",
			NewSubscriberErrorDetail("registry unavailable", req.SubscriberID, true),
		)
	}

	subscriberID, err := model.CanonicalizeSubscriberIdentifier(strings.TrimSpace(req.SubscriberID))
	if err != nil {
		// A MALFORMED IDENTIFIER IS A 400, not a 401, and that is not an enumeration leak:
		// the answer depends only on the shape of the value the caller sent, which it can
		// determine for itself, and it says nothing about which subscribers exist.
		return nil, apierror.NewAPIError(
			apierror.ErrGenValidation,
			err.Error(),
			NewSubscriberErrorDetail("invalid subscriber identifier", req.SubscriberID, false),
		)
	}

	if strings.TrimSpace(req.Secret) == "" {
		return nil, streamUnauthenticated(subscriberID, "no credential was presented")
	}

	subscriber, err := g.registry.GetEventSubscriberByID(ctx, subscriberID)
	if err != nil {
		// A NOT-FOUND ROW IS FLATTENED INTO THE REFUSAL; every other failure — a dropped
		// connection, an expired budget — is reported as itself, because a caller CAN act on
		// those and they disclose nothing about the registry's contents.
		if isSubscriberNotFoundError(err) {
			return nil, streamUnauthenticated(subscriberID, "no such subscriber")
		}

		return nil, err
	}

	if !subscriber.IsProvisioned() {
		return nil, streamUnauthenticated(subscriberID, "the subscriber holds no credential")
	}

	if presented := strings.TrimSpace(req.Principal); presented != "" && presented != subscriber.KafkaPrincipal {
		return nil, streamUnauthenticated(subscriberID, "the presented principal is not this subscriber's")
	}

	if subscriber.RevocationPendingAt != nil {
		// FAIL CLOSED WHILE A REVOCATION IS OUTSTANDING. The registry has recorded that this
		// credential's access is being withdrawn and could not confirm that it was; serving
		// records under it until the settlement succeeds would keep the one door open that an
		// operator has already asked to close.
		return nil, streamUnauthenticated(subscriberID, "the subscriber's credential is pending revocation")
	}

	if !model.CredentialReferenceMatches(*subscriber.CredentialReference, subscriber.KafkaPrincipal, req.Secret) {
		return nil, streamUnauthenticated(subscriberID, "the presented secret does not match the recorded credential")
	}

	return subscriber, nil
}

// authorizeTopic proves the authenticated subscriber may read the requested topic.
//
// # Why the grant is checked HERE and not left to the broker
//
// The gateway reads with Blnk's own credential, which is wide enough to read every category
// topic. The broker will therefore allow any topic this process asks for, so the subscriber's
// topic grant — a dimension Kafka DOES enforce on the direct path — has no enforcement on
// this path unless this function provides it. Omitting the check would make the gateway a
// hole in a boundary the rest of the system keeps.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the authenticated row.
//   - requested string: the topic the caller asked for.
//
// Returns:
//   - string: the trimmed topic.
//   - error: ErrSubscriberTopicNotGranted when the topic is outside the grant or is one no
//     subscriber may be granted; a validation error when none was named.
func (g *SubscriberStreamGateway) authorizeTopic(
	subscriber *model.EventSubscriber,
	requested string,
) (string, error) {
	topic := strings.TrimSpace(requested)
	if topic == "" {
		return "", apierror.NewAPIError(
			apierror.ErrGenMissingParameter,
			"A topic is required: name one of the subscriber's authorized_topics",
			NewSubscriberErrorDetail("no topic named", subscriber.SubscriberID, false),
		)
	}

	// THE GRANTABILITY RULE FIRST, so a dead-letter topic or the internal system category is
	// refused even if a registry row somehow names one. It is the same rule registration
	// applies, read from the same place, so the two cannot disagree about what a subscriber
	// may be granted.
	if !IsSubscriberGrantableTopic(topic) {
		return "", streamTopicNotGranted(subscriber, topic, "no subscriber may be granted this topic")
	}

	if !subscriber.HasTopicAccess(topic) {
		return "", streamTopicNotGranted(subscriber, topic, "the topic is outside the subscriber's grant")
	}

	return topic, nil
}

// boundedFetch turns a request into a bounded broker read, clamping every bound.
//
// Clamping rather than refusing, for the two size bounds: a caller asking for a larger page
// or a longer wait is expressing a throughput preference, and the response reports what was
// actually read, so serving it at the ceiling is both safe and more useful than a 400. An
// unrecognised OFFSET is refused instead, because there is no safe interpretation of one — a
// silent substitution would return the wrong records rather than fewer of them.
//
// Parameters:
//   - req SubscriberStreamRequest: the caller's request.
//   - topic string: the authorised topic.
//
// Returns:
//   - TopicRecordFetch: the bounded read.
//   - error: a validation error for a negative partition or an unrecognised offset;
//     ErrKafkaUnavailable when no broker is configured.
func (g *SubscriberStreamGateway) boundedFetch(
	req SubscriberStreamRequest,
	topic string,
) (TopicRecordFetch, error) {
	fetch := TopicRecordFetch{Topic: topic}

	if g.fetcher == nil || !g.fetcher.IsConfigured() {
		return fetch, apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"No Kafka broker is configured, so there is no event stream to read. Set KAFKA_BROKERS",
			NewSubscriberErrorDetail(ErrSubscriberStreamUnavailable.Error(), req.SubscriberID, true),
		)
	}

	if req.Partition < 0 {
		return fetch, apierror.NewAPIError(
			apierror.ErrGenValidation,
			fmt.Sprintf("partition must be zero or greater, got %d", req.Partition),
			NewSubscriberErrorDetail("invalid partition", req.SubscriberID, false),
		)
	}

	if req.Offset < 0 &&
		req.Offset != SubscriberStreamOffsetEarliest &&
		req.Offset != SubscriberStreamOffsetLatest {
		return fetch, apierror.NewAPIError(
			apierror.ErrGenValidation,
			fmt.Sprintf(
				"offset must be zero or greater, %d for the earliest retained record, or %d for the "+
					"end of the log; got %d",
				SubscriberStreamOffsetEarliest, SubscriberStreamOffsetLatest, req.Offset,
			),
			NewSubscriberErrorDetail("invalid offset", req.SubscriberID, false),
		)
	}

	fetch.Partition = req.Partition
	fetch.Offset = req.Offset
	fetch.MaxRecords = clampStreamLimit(req.Limit)
	fetch.MaxBytes = MaxSubscriberStreamBytes
	fetch.MaxWait = clampStreamMaxWait(req.MaxWait)

	return fetch, nil
}

// filter applies the key scope to every record and assembles the page.
//
// # This is the boundary
//
// Every record the broker returned passes through model.EventSubscriber.HasKeyAccess, which
// is the same predicate the credential contract and every response field describe. A record
// it refuses is DROPPED — not redacted, not summarised, not counted anywhere the caller can
// reconstruct it from — and only its existence is reflected, as a number.
//
// The cursor advances past withheld records, which is what makes the boundary usable rather
// than merely correct: a subscriber whose entitled records sit behind a thousand belonging to
// other ledgers would otherwise never reach them.
//
// Parameters:
//   - subscriber *model.EventSubscriber: the authenticated row, whose key scope is applied.
//   - topic string: the topic read.
//   - fetch TopicRecordFetch: the read that produced the batch, for the requested offset.
//   - batch TopicRecordBatch: what the broker returned.
//
// Returns:
//   - SubscriberStreamPage: the records the subscriber is entitled to and its cursor.
func (g *SubscriberStreamGateway) filter(
	subscriber *model.EventSubscriber,
	topic string,
	fetch TopicRecordFetch,
	batch TopicRecordBatch,
) SubscriberStreamPage {
	page := SubscriberStreamPage{
		SubscriberID:     subscriber.SubscriberID,
		Topic:            topic,
		Partition:        fetch.Partition,
		Records:          make([]SubscriberStreamRecord, 0, len(batch.Records)),
		NextOffset:       batch.NextOffset(fetch.Offset),
		HighWatermark:    batch.HighWatermark,
		LogStartOffset:   batch.LogStartOffset,
		RecordsScanned:   len(batch.Records),
		Truncated:        batch.Truncated,
		KeyScope:         subscriber.RequestedKeyScope(),
		KeyScopeEnforced: subscriber.RequiresGatewayDelivery(),
	}

	for _, record := range batch.Records {
		key := string(record.Key)

		// THE AUTHORIZATION RULE, from the model, per record. Not a prefix comparison written
		// here: a second implementation of the rule is a second thing that can drift from what
		// the registry recorded and from what every response promises.
		if !subscriber.HasKeyAccess(key) {
			page.RecordsWithheld++

			continue
		}

		page.Records = append(page.Records, SubscriberStreamRecord{
			Offset:    record.Offset,
			Partition: fetch.Partition,
			Key:       key,
			Timestamp: record.Time,
			Value:     record.Value,
		})
	}

	recordStreamGatewayOutcome(subscriber, topic, page)

	return page
}

// clampStreamLimit resolves a requested page size to one within the bounds.
func clampStreamLimit(requested int) int {
	if requested <= 0 {
		return DefaultSubscriberStreamLimit
	}

	if requested > MaxSubscriberStreamLimit {
		return MaxSubscriberStreamLimit
	}

	return requested
}

// clampStreamMaxWait resolves a requested wait to one within the bounds.
func clampStreamMaxWait(requested time.Duration) time.Duration {
	if requested <= 0 {
		return DefaultSubscriberStreamMaxWait
	}

	if requested > MaxSubscriberStreamMaxWait {
		return MaxSubscriberStreamMaxWait
	}

	return requested
}

// streamUnauthenticated builds the ONE refusal every authentication failure returns, and logs
// the reason the caller is not told.
//
// The split is the whole point: the operator gets the cause, the caller gets a verdict. The
// identifier is hashed in the log line for the same reason every other subscriber log field
// is — it is a tenant-chosen name — and the presented secret appears nowhere at all.
//
// Parameters:
//   - subscriberID string: the canonical identifier, for the log line only.
//   - reason string: why the credential was refused. Never returned to the caller.
//
// Returns:
//   - error: apierror.ErrSubscriberCredentialInvalid with a caller-safe message.
func streamUnauthenticated(subscriberID, reason string) error {
	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriberID),
		"reason":             reason,
	}).Warn(
		"subscriber stream: refused a read because the presented credential could not be " +
			"authenticated. The caller is told only that the credential is invalid — a distinct " +
			"answer per cause would let an unauthenticated caller enumerate the registry — so this " +
			"line is the only place the reason is recorded",
	)

	return apierror.NewAPIError(
		apierror.ErrSubscriberCredentialInvalid,
		"The presented subscriber credential is not valid. Issue credentials with "+
			"POST /subscribers/{subscriber_id}/kafka-credentials and present the returned username "+
			"and password",
		// NOT retryable: repeating the request with the same secret cannot succeed, and a
		// client that treated this as transient would loop.
		NewSubscriberErrorDetail(ErrSubscriberStreamUnauthenticated.Error(), subscriberID, false),
	)
}

// streamTopicNotGranted builds the refusal an authenticated subscriber gets for a topic
// outside its grant.
//
// Unlike the authentication refusal this one NAMES the topic, and may: the caller sent it,
// and a caller that has authenticated as this subscriber can already read its own grant
// through GET /subscribers/{id}. Withholding it here would cost an operator the one fact that
// makes the refusal actionable while protecting nothing.
func streamTopicNotGranted(subscriber *model.EventSubscriber, topic, reason string) error {
	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
		"principal_hash":     subscriberLogLabel(subscriber.KafkaPrincipal),
		"topic":              topic,
		"authorized_topics":  len(subscriber.AuthorizedTopics),
		"reason":             reason,
	}).Warn(
		"subscriber stream: refused a read of a topic outside the subscriber's grant. This is the " +
			"topic dimension enforced HERE rather than at the broker: the gateway reads with Blnk's " +
			"own credential, which can read every category topic, so the subscriber's grant has no " +
			"other enforcement on this path",
	)

	return apierror.NewAPIError(
		apierror.ErrSubscriberTopicNotGranted,
		fmt.Sprintf(
			"This subscriber is not granted topic %q. Read GET /subscribers/%s for its "+
				"authorized_topics, or ask an operator to widen the grant",
			topic, subscriber.SubscriberID,
		),
		// NOT retryable: only an authorization change can make this succeed.
		NewSubscriberErrorDetail(ErrSubscriberStreamTopicNotGranted.Error(), subscriber.SubscriberID, false),
	)
}

// streamFetchFailure classifies a broker read failure for the caller.
//
// A refused read and an unreachable broker are both upstream conditions rather than defects
// here, so both resolve to 503 with the retryable flag set — but the DETAIL distinguishes
// them, because the operator action is different: one is an ACL to look at, the other a
// broker to bring back.
func streamFetchFailure(subscriber *model.EventSubscriber, topic string, err error) error {
	if errors.Is(err, ErrKafkaAdminNotConfigured) {
		return apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"No Kafka broker is configured, so there is no event stream to read. Set KAFKA_BROKERS",
			NewSubscriberErrorDetail(ErrSubscriberStreamUnavailable.Error(), subscriber.SubscriberID, true),
		)
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash": subscriberLogLabel(subscriber.SubscriberID),
		"topic":              topic,
		"error":              err.Error(),
	}).Error(
		"subscriber stream: the broker read failed, so the subscriber was answered with an " +
			"unavailable stream rather than an empty page — an empty page would read as 'caught up' " +
			"and advance nothing, which is indistinguishable from a healthy quiet topic",
	)

	return apierror.NewAPIError(
		apierror.ErrKafkaUnavailable,
		"The event stream could not be read from Kafka. Retry; if it persists, check the broker's "+
			"health and Blnk's own read grant on this topic",
		// RETRYABLE: the request is well formed and the condition is upstream.
		NewSubscriberErrorDetail("the broker read failed", subscriber.SubscriberID, true),
	)
}

// recordStreamGatewayOutcome counts what the gateway delivered and what it withheld.
//
// # Why withheld is a first-class measurement
//
// It is the only externally visible evidence that the boundary is doing anything. A
// key-scoped subscriber's feed looks identical whether the filter is working or the topic is
// simply quiet, so an operator asked "is isolation being enforced?" has nothing to read
// unless the withheld count is recorded. A sustained delivered count with a zero withheld
// count on a shared topic is the signature of a filter that has stopped filtering.
//
// # Why the attributes stop at the topic
//
// A subscriber id is caller-chosen and unbounded, and a counter attributed by one grows a
// time series per subscriber for the lifetime of the process. The consumer-lag gauge carries
// subscriber identity because a lag figure is meaningless without it and it has an explicit
// budget; a delivered/withheld count is useful per topic and per outcome, so it stops there.
func recordStreamGatewayOutcome(
	subscriber *model.EventSubscriber,
	topic string,
	page SubscriberStreamPage,
) {
	if metrics.SubscriberStreamRecordsDelivered == nil || metrics.SubscriberStreamRecordsWithheld == nil {
		// Metrics are initialised by the server and worker roles; a unit test or a CLI
		// invocation legitimately runs without them, and a gateway that panicked here would
		// make observability a hard dependency of correctness.
		return
	}

	attributes := metric.WithAttributes(
		attribute.String("topic", topic),
		attribute.Bool("key_scoped", subscriber.RequiresGatewayDelivery()),
	)

	if delivered := len(page.Records); delivered > 0 {
		metrics.SubscriberStreamRecordsDelivered.Add(context.Background(), int64(delivered), attributes)
	}

	if page.RecordsWithheld > 0 {
		metrics.SubscriberStreamRecordsWithheld.Add(context.Background(), int64(page.RecordsWithheld), attributes)
	}
}

// ---------------------------------------------------------------------------------------
// The Blnk entry point
// ---------------------------------------------------------------------------------------

// SubscriberStream returns a gateway bound to this instance's registry and administrative
// client.
//
// The administrative client is BORROWED from the instance rather than built here, so a stream
// read costs no transport, no connection per broker and no SASL handshake. It is therefore
// not closed by the gateway — the instance owns it, and Blnk.Close returns it.
//
// A nil instance, or one with no broker configured, yields a gateway that reports the stream
// unavailable. That is deliberate and is the same answer the credential endpoint gives: a
// deployment with no Kafka publishes nothing, so there is nothing to stream, and an empty
// page would tell a subscriber it was caught up.
//
// Returns:
//   - *SubscriberStreamGateway: the gateway, never nil.
func (b *Blnk) SubscriberStream() *SubscriberStreamGateway {
	if b == nil {
		return NewSubscriberStreamGateway(nil, nil)
	}

	admin, err := b.KafkaAdmin()
	if err != nil || admin == nil {
		if err != nil {
			kafkaErrorEntry("subscriber_stream_admin", err).Warn(
				"subscriber stream: no administrative client could be resolved, so stream reads " +
					"report the stream unavailable rather than an empty page",
			)
		}

		return NewSubscriberStreamGateway(b.datasource, nil)
	}

	return NewSubscriberStreamGateway(b.datasource, admin)
}

// ReadSubscriberEventStream is the read behind GET /subscribers/{id}/events.
//
// It is the whole of the API layer's dependency on the gateway, so the handler never
// constructs one and never touches the administrative client.
//
// Parameters:
//   - ctx context.Context: bounds the read; the handler applies SubscriberStreamBudget.
//   - req SubscriberStreamRequest: what to read and the credential to read it with.
//
// Returns:
//   - SubscriberStreamPage: the records the subscriber is entitled to.
//   - error: the gateway's typed refusals, unchanged.
func (b *Blnk) ReadSubscriberEventStream(
	ctx context.Context,
	req SubscriberStreamRequest,
) (SubscriberStreamPage, error) {
	return b.SubscriberStream().ReadEvents(ctx, req)
}
