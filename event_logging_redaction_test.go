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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/internal/logsafe"
	"github.com/blnkfinance/blnk/model"
)

// This file covers WHAT THE EVENT PIPELINE'S LOG LINES DISCLOSE, as distinct from what they
// report. Every other test about logging in this package asserts that a required field is
// present; these assert that a forbidden value is absent, which needs its own file because
// the two kinds of assertion fail for opposite reasons and a reader must not have to
// disentangle them.
//
// The disclosure this guards is a broker's address. A kafka-go error renders as
// "dial tcp 10.0.3.14:9092: connect: connection refused", and the pipeline logs such an
// error on every failed publish attempt, on every failed administrative call, on every
// lease-renewal failure and at start-up. A log is retained, shipped onward and readable by
// more people than the deployment's operators, so an address in one is reconnaissance that
// outlives the incident that produced it.
//
// The invariant every test here defends is the SPLIT: a normal-level line carries the
// diagnosis with the topology removed, and the verbatim text is reachable only at debug.
// Losing either half is a defect — without the first the address is published, and without
// the second an operator cannot diagnose a broker they cannot name.

// errKafkaDialFailure is the error shape every assertion in this file is built on: a real
// kafka-go dial failure, whose text is one part diagnosis and one part topology.
var errKafkaDialFailure = errors.New("dial tcp 10.0.3.14:9092: connect: connection refused")

// TestWithLoggableCause_SplitsRedactedFromVerbatim pins the helper every log site in this
// package routes a dependency error through.
func TestWithLoggableCause_SplitsRedactedFromVerbatim(t *testing.T) {
	t.Run("a normal level carries the diagnosis without the address", func(t *testing.T) {
		captured := captureLogs(t, logrus.InfoLevel, func() {
			withLoggableCause(nil, errKafkaDialFailure).Error("event relay: publish failed")
		})

		cause, isString := captured.field("publish failed", "cause").(string)
		require.True(t, isString, "the cause field must be present: %s", captured.raw)
		assert.Contains(t, cause, "connection refused",
			"the diagnosis must survive, or the line answers nothing")
		assert.Contains(t, cause, logsafe.Placeholder)
		assert.NotContains(t, captured.raw, "10.0.3.14",
			"the broker address must be absent from the WHOLE record, not merely from one field")
		assert.Nil(t, captured.field("publish failed", "cause_verbatim"),
			"the unredacted rendering belongs only in the debug sink")
		assert.Nil(t, captured.field("publish failed", logrus.ErrorKey),
			"logrus's own error field is what rendered the address verbatim; it must not return")
	})

	t.Run("debug adds the verbatim rendering", func(t *testing.T) {
		captured := captureLogs(t, logrus.DebugLevel, func() {
			withLoggableCause(nil, errKafkaDialFailure).Error("event relay: publish failed")
		})

		verbatim, isString := captured.field("publish failed", "cause_verbatim").(string)
		require.True(t, isString, "debug must carry the verbatim rendering: %s", captured.raw)
		assert.Contains(t, verbatim, "10.0.3.14:9092",
			"an operator who turned debug on has asked for exactly this")
	})

	t.Run("the caller's own fields are preserved", func(t *testing.T) {
		captured := captureLogs(t, logrus.InfoLevel, func() {
			withLoggableCause(
				logrus.WithField("event_id", "evt_1234"), errKafkaDialFailure,
			).Warn("event relay: lease renewal failed")
		})

		assert.Equal(t, "evt_1234", captured.field("lease renewal failed", "event_id"))
	})

	t.Run("a nil error leaves the entry untouched", func(t *testing.T) {
		captured := captureLogs(t, logrus.InfoLevel, func() {
			withLoggableCause(logrus.WithField("event_id", "evt_1234"), nil).
				Info("event relay: nothing failed")
		})

		assert.Equal(t, "evt_1234", captured.field("nothing failed", "event_id"))
		assert.Nil(t, captured.field("nothing failed", "cause"))
	})

	t.Run("a nil entry is accepted", func(t *testing.T) {
		captured := captureLogs(t, logrus.InfoLevel, func() {
			withLoggableCause(nil, nil).Info("event relay: no entry and no error")
		})

		assert.Equal(t, 1, captured.count("no entry and no error"))
	})
}

// TestRelayReasonRenderings_DifferByDestination is the invariant behind the two renderings,
// and the one most likely to be undone by a future edit that "unifies" them.
//
// The durable rendering keeps the broker's own words because its two readers — the
// last_error column and the failure metadata on a `<topic>.dlt` sibling, which subscribers
// cannot be granted — are already privileged, and the address is the most useful part of a
// dead-letter triage. The log rendering removes it because a log's readership is wider.
func TestRelayReasonRenderings_DifferByDestination(t *testing.T) {
	durable := relayFailureReason(errKafkaDialFailure)
	logged := relayLogReason(errKafkaDialFailure)

	assert.Contains(t, durable, "10.0.3.14:9092",
		"the stored reason must keep the address a dead-letter triage needs")
	assert.NotContains(t, logged, "10.0.3.14",
		"the logged reason must not publish the address")
	assert.Contains(t, logged, "connection refused",
		"the logged reason must still say what happened")

	assert.Equal(t, durable, relayFailureReason(errKafkaDialFailure),
		"both renderings must be deterministic")

	t.Run("both agree about the absence of a reason", func(t *testing.T) {
		assert.Equal(t, relayFailureReason(nil), relayLogReason(nil),
			"a missing reason must read identically in both sinks")
		assert.NotEmpty(t, relayFailureReason(nil),
			"an empty reason on a failed attempt is indistinguishable from an untried row")
	})

	t.Run("a whitespace-only error still yields a reason", func(t *testing.T) {
		assert.NotEmpty(t, relayLogReason(errors.New("   ")),
			"sanitization must not be able to turn a reason into an empty string")
	})

	t.Run("both bound an oversized error", func(t *testing.T) {
		oversized := errors.New(string(make([]byte, 0, 4096)) + fmt.Sprintf("%04096d", 1))

		assert.LessOrEqual(t, len([]rune(relayLogReason(oversized))),
			logsafe.MaxErrorLength+len([]rune(logsafe.TruncationSuffix)))
		assert.LessOrEqual(t, len([]rune(relayFailureReason(oversized))),
			logsafe.MaxErrorLength+len([]rune(logsafe.TruncationSuffix)))
	})
}

// TestPublishResultLogFields_RedactsTheTransportCause covers the per-attempt line requirement
// R-4 mandates. It is emitted once per attempt per event, so it is simultaneously the most
// frequent line in the pipeline and the one most likely to carry a transport error.
func TestPublishResultLogFields_RedactsTheTransportCause(t *testing.T) {
	result := PublishResult{
		Status:       model.PublishStatusRetrying,
		Topic:        "blnk.transactions",
		EventID:      "evt_9f2b",
		EventType:    "transaction.applied",
		PartitionKey: "ldg_secret_identifier",
		Attempt:      2,
		MaxAttempts:  5,
		Duration:     15 * time.Millisecond,
		Transient:    true,
		Retryable:    true,
		Err:          errKafkaDialFailure,
	}

	fields := result.LogFields()

	reason, isString := fields["error"].(string)
	require.True(t, isString, "a failed attempt must report a reason")
	assert.Contains(t, reason, "connection refused")
	assert.NotContains(t, reason, "10.0.3.14",
		"the per-attempt line is the most frequent line in the pipeline; it must not carry an address")

	assert.NotContains(t, fmt.Sprint(fields), "ldg_secret_identifier",
		"the partition key is a financial identifier and is reported hashed")

	t.Run("a successful attempt reports no reason", func(t *testing.T) {
		success := PublishResult{Status: model.PublishStatusDispatched, Topic: "blnk.transactions"}

		assert.Nil(t, success.LogFields()["error"])
	})
}

// TestEventPipelineLogging_NoSiteUsesLogrusWithError is a source-level guard.
//
// Every behavioural test above proves one site behaves; this proves no site was MISSED, and
// that a new one cannot quietly reintroduce the defect. logrus.WithError renders err.Error()
// verbatim into the record at whatever level the line is emitted at, which is precisely the
// disclosure withLoggableCause exists to prevent — so its absence from these files is the
// invariant, and it is cheaper to assert here than to rediscover in a review.
func TestEventPipelineLogging_NoSiteUsesLogrusWithError(t *testing.T) {
	// Repository-relative, so the one guard covers every layer the pipeline spans: the
	// services, the repositories beneath them and the process wiring above them. A
	// redaction that holds in the service and not in the repository underneath it protects
	// nothing, because the same failure is logged at both.
	//
	// webhooks.go is deliberately absent. Its single site is converted, but the file is
	// DELETED at the sunset, and a guard naming it would turn that deletion into a test
	// failure for a file that no longer exists.
	files := []string{
		"event_relay.go",
		"event_admin.go",
		"event_publisher.go",
		"event_publisher_telemetry.go",
		"event_dlt.go",
		"event_outbox.go",
		"event_subscriber.go",
		"event_metrics.go",
		"event_sunset.go",
		"event_topics.go",
		"event_retention.go",
		"database/event_outbox.go",
		"database/event_subscriber.go",
		"cmd/server.go",
	}

	for _, name := range files {
		t.Run(name, func(t *testing.T) {
			// The OFFENDING LINES are reported rather than the file. Asserting on the
			// whole source would put an entire event-pipeline file into the failure
			// output, which buries the one line that has to change.
			var offending []string

			for number, line := range strings.Split(readRepoFile(t, name), "\n") {
				if strings.Contains(line, "WithError(") {
					offending = append(offending,
						fmt.Sprintf("%s:%d: %s", name, number+1, strings.TrimSpace(line)))
				}
			}

			assert.Emptyf(t, offending,
				"these sites must log through withLoggableCause, which redacts network topology "+
					"and bounds the text, rather than through logrus.WithError, which renders the "+
					"error verbatim at whatever level the line is emitted at:\n%s",
				strings.Join(offending, "\n"))
		})
	}
}
