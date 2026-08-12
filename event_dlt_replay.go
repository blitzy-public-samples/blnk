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

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// ReplayDeadLetteredEvent re-publishes a dead-lettered event to the topic it was
// originally destined for.
//
// Parameters:
//   - ctx context.Context: cancels the lookup, the publish and the recording.
//   - eventID string: the event's UUID, as listed by the inventory.
//
// Returns:
//   - ReplayOutcome: the record of the replay.
//   - error: ErrGenValidation for a blank id, ErrEventNotFound when no such event
//     exists, ErrEventNotDeadLettered when the event is not in the dead-lettered state,
//     ErrKafkaUnavailable when there is no transport or the broker is unavailable, or
//     ErrEventReplayFailed when the re-publish fails for any other reason and when the
//     event was republished but its row could not be marked dispatched.
func (s *EventDeadLetterService) ReplayDeadLetteredEvent(
	ctx context.Context,
	eventID string,
) (ReplayOutcome, error) {
	ctx, span := tracer.Start(ctx, "ReplayDeadLetteredEvent")
	defer span.End()

	if s == nil || s.store == nil {
		return ReplayOutcome{}, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Replaying a dead-lettered event requires a datasource",
			errors.New("blnk: the dead-letter service has no datasource"),
		)
	}

	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return ReplayOutcome{}, apierror.NewAPIError(
			apierror.ErrGenValidation,
			"An event id is required to replay a dead-lettered event",
			errors.New("blnk: replay called with a blank event id"),
		)
	}
	span.SetAttributes(attribute.String("event.id", eventID))

	// CLAIMED, not merely read. The claim is what makes concurrent replay safe; see
	// claimReplayableEvent and the eventDeadLetterStore contract.
	row, err := s.claimReplayableEvent(ctx, eventID)
	if err != nil {
		span.RecordError(err)

		return ReplayOutcome{}, err
	}

	// From here on the row is held in the replaying state, so EVERY exit path must either
	// mark it dispatched or release the claim. Releasing is idempotent from the caller's
	// point of view — releaseReplayClaim reports its own failures and never masks the
	// error being returned — so the deferred-style guard below is safe to pair with the
	// explicit success transition further down.
	released := false
	releaseOnFailure := func(reason error) {
		if released {
			return
		}
		released = true
		s.releaseReplayClaim(ctx, row, reason)
	}

	topic := ReplayTopicFor(*row)
	attempt := replayAttemptNumber(*row)

	span.SetAttributes(
		attribute.String("event.type", row.EventType),
		attribute.String("event.topic", topic),
		attribute.String("event.dlt_topic", row.DLTTopic),
		attribute.Int("event.replay_attempt", attempt),
	)

	publisher, _, err := s.transport()
	if err != nil {
		span.RecordError(err)
		releaseOnFailure(err)

		return ReplayOutcome{}, err
	}

	if IsNoopEventPublisher(publisher) {
		err = apierror.NewAPIError(
			apierror.ErrKafkaUnavailable,
			"No Kafka broker is configured, so the event cannot be replayed",
			fmt.Errorf("blnk: replay of event %q requires KAFKA_BROKERS to be configured", eventID),
		)
		span.RecordError(err)
		releaseOnFailure(err)

		return ReplayOutcome{}, err
	}

	// The request is built from the stored row, so the bytes on the wire are the bytes the
	// first publish produced. Only the destination and the attempt label are stated here,
	// and neither takes part in the message value.
	request := PublishRequestFromOutbox(*row, attempt)
	request.Topic = topic

	// A replay is NOT attempt N+1 of a live retry sequence — that sequence ended when the
	// event was dead-lettered. Stating the purpose is what keeps it out of the attempt="1"
	// latency population the sub-2-second target is read from, and out of the published-
	// events counter that is the denominator of the dead-letter rate.
	request.Purpose = PublishPurposeReplay

	result, publishErr := publisher.PublishToTopic(ctx, request)

	outcome := ReplayOutcome{
		EventID:      row.EventID,
		EventType:    row.EventType,
		Topic:        result.Topic,
		PartitionKey: result.PartitionKey,
		Status:       result.Status,
		ReplayedAt:   s.now().UTC(),
		Result:       result,
	}

	if publishErr != nil {
		// Same boundary as the dead-letter write: the publisher's error carries the broker's
		// own words — and, through *net.OpError, its address — so the log line below keeps
		// them while the caller receives the bounded diagnosis.
		code, message := replayFailureOutcome(publishErr)
		detail := NewEventTransportErrorDetail(
			"the Kafka broker did not acknowledge the replayed message",
			eventID, row.EventType, topic, publishErr,
		)
		// The detail's retryability is taken from the SAME verdict as the code, and not from
		// the detail constructor's own narrower classifier. Those two answer very nearly the
		// same question but not identically — the constructor does not know about a closed
		// transport, about BrokerNotAvailable, or about a PublishError's explicit verdict —
		// so leaving them independent would let one response say 503 in its status and
		// "transient": false in its body. A caller deciding whether to retry reads whichever
		// it happens to trust, and half of them would be wrong.
		detail.Transient = code == apierror.ErrKafkaUnavailable

		err = apierror.NewAPIError(code, message, detail)
		span.RecordError(err)
		logrus.WithFields(outcome.LogFields()).WithField("failure_detail_class", classifyDeadLetterFailure(errorText(publishErr))).
			Error("replaying a dead-lettered ledger event failed")
		// The publish failed, so the row is owed nothing further and must go back to
		// dead_lettered — otherwise a failed replay would cost the event its replayability by
		// stranding it in replaying.
		releaseOnFailure(publishErr)

		return outcome, err
	}

	if markErr := s.store.MarkEventDispatched(
		ctx, row.ID, row.ClaimToken, result.Record,
	); markErr != nil {
		// The event HAS been republished. The bookkeeping has not, so the row is still listed
		// as dead-lettered and can be replayed again — a duplicate that the subscriber's
		// idempotency on the unchanged event id absorbs. An error is returned rather than
		// swallowed precisely because the operator must know the entry has not cleared;
		// reporting success here would leave a phantom in the inventory with nobody looking
		// for it.
		err = apierror.NewAPIError(
			apierror.ErrEventReplayFailed,
			"The event was republished but its outbox entry is still marked dead-lettered",
			fmt.Errorf("blnk: marking replayed event %q dispatched: %w", eventID, markErr),
		)
		span.RecordError(err)
		logrus.WithFields(outcome.LogFields()).WithField("failure_detail_class", classifyDeadLetterFailure(errorText(markErr))).
			Error("a replayed ledger event could not be marked dispatched")
		// The event IS on the topic but the success transition did not land, so the row is
		// returned to dead_lettered rather than left in replaying. That keeps the entry
		// visible in the inventory — which is what the operator needs, since the error above
		// tells them it has not cleared — instead of hiding it in a state no view reports on.
		releaseOnFailure(markErr)

		return outcome, err
	}
	released = true

	outcome.Recorded = true
	logrus.WithFields(outcome.LogFields()).Info("dead-lettered ledger event replayed to its original topic")

	return outcome, nil
}

// replayClaimLease is how long a replay holds its claim on a row.
const replayClaimLease = 2 * time.Minute

// replayFailureOutcome maps a failed re-publish onto the typed code and the
// operator-facing message the replay endpoint must answer with.
func replayFailureOutcome(cause error) (apierror.ErrorCode, string) {
	if localContextTermination(cause) {
		return apierror.ErrEventReplayFailed,
			"The replay was abandoned before the broker acknowledged it; the event is still " +
				"dead-lettered, so the request can be repeated"
	}

	if IsBrokerUnavailableError(cause) {
		return apierror.ErrKafkaUnavailable,
			"The Kafka broker is unavailable, so the event could not be replayed; retry once it recovers"
	}

	return apierror.ErrEventReplayFailed, "Failed to replay the dead-lettered event to its original topic"
}

// claimReplayableEvent CLAIMS a dead-lettered row for replay and translates the
// repository's failures into the typed errors this API answers with.
func (s *EventDeadLetterService) claimReplayableEvent(
	ctx context.Context,
	eventID string,
) (*model.EventOutbox, error) {
	row, err := s.store.ClaimEventForReplay(ctx, eventID, replayClaimLease)
	if err != nil {
		if isNotFoundError(err) {
			return nil, apierror.NewAPIError(
				apierror.ErrEventNotFound,
				"No event with that id exists",
				fmt.Errorf("blnk: event %q not found: %w", eventID, err),
			)
		}
		if isConflictError(err) {
			// The row exists but is not dead-lettered. WHICH state it is in decides what the
			// operator is told, because "already replayed", "still being delivered" and "another
			// replay is in flight" are three different situations and only one of them is a
			// mistake.
			return nil, s.describeUnreplayableEvent(ctx, eventID, err)
		}

		return nil, err
	}

	if row == nil {
		// Defensive: the repository returns a typed error rather than a nil row, and
		// this keeps a future change to that contract from becoming a nil dereference.
		return nil, apierror.NewAPIError(
			apierror.ErrEventNotFound,
			"No event with that id exists",
			fmt.Errorf("blnk: event %q not found", eventID),
		)
	}

	return row, nil
}

// describeUnreplayableEvent turns a refused replay claim into the message that names
// the operator's actual situation.
func (s *EventDeadLetterService) describeUnreplayableEvent(ctx context.Context, eventID string, cause error) error {
	message := "Only a dead-lettered event can be replayed"

	if row, err := s.store.GetEventByID(ctx, eventID); err == nil && row != nil {
		switch {
		case row.Status == model.EventOutboxStatusDispatched && row.DLTTopic != "":
			message = "This event has already been replayed and cannot be replayed again"
		case row.Status == model.EventOutboxStatusReplaying:
			message = "This event is already being replayed; wait for that replay to finish"
		default:
			message = fmt.Sprintf("Only a dead-lettered event can be replayed; this one is %s", row.Status)
		}
	}

	return apierror.NewAPIError(
		apierror.ErrEventNotDeadLettered,
		message,
		fmt.Errorf("blnk: event %q could not be claimed for replay: %w", eventID, cause),
	)
}

// releaseReplayClaim returns a claimed row to dead_lettered after a replay that did not
// complete.
func (s *EventDeadLetterService) releaseReplayClaim(ctx context.Context, row *model.EventOutbox, reason error) {
	if row == nil {
		return
	}

	var replayErr string
	if reason != nil {
		replayErr = sanitizeLogValue(reason.Error(), maxLoggedErrorLength)
	}

	release, cancel := context.WithTimeout(context.WithoutCancel(ctx), replayReleaseTimeout)
	defer cancel()

	if err := s.store.ReleaseEventReplay(release, row.ID, row.ClaimToken, replayErr); err != nil {
		withLoggableCause(logrus.WithFields(logrus.Fields{
			"event_id":   row.EventID,
			"event_type": row.EventType,
			"dlt_topic":  row.DLTTopic,
		}), err).Error(
			"a claimed replay could not be returned to the dead-lettered state; it clears when its " +
				"claim lease expires, because the replay claim reclaims an expired claim atomically",
		)
	}
}

// replayReleaseTimeout bounds the detached rollback of a replay claim. One UPDATE, so it
// matches the relay's bookkeeping budget: long enough for a healthy database, short enough
// that a database that has gone away cannot hold a request open on work whose failure the
// claim's own lease recovery already covers.
const replayReleaseTimeout = 5 * time.Second
