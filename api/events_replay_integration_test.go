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

// events_replay_integration_test.go holds the ONE assertion about the event management
// surface that cannot be made without a broker: that a 200 from POST
// /events/dead-letter/:event_id/replay actually put the record back on the topic it
// came from, keyed as the original was and carrying the original bytes.
//
// events_api_test.go covers the request-side contract of this surface — the master-key
// gate, the closed query-parameter set, parsing, validation and the response envelope —
// and it does so with NO broker configured, which is both the cheaper posture and the
// deployment posture that has to keep working.
//
// Splitting it is the smaller correction of the two available.
package api

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/config"
	coremodel "github.com/blnkfinance/blnk/model"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// eventsReplayKafkaClient builds an authenticated Kafka client for reading a replayed
// record back off its topic.
//
// The local stack speaks SASL/SCRAM-SHA-512, so an unauthenticated client cannot read
// anything and a test built on one would report every assertion as a broker problem.
//
// Parameters:
//   - t *testing.T: the test, failed when the mechanism cannot be built.
//   - kafkaConfig config.KafkaConfig: the resolved broker environment.
//
// Returns:
//   - *kafka.Client: a client addressed at the first broker, with its transport closed
//     on cleanup.
func eventsReplayKafkaClient(t *testing.T, kafkaConfig config.KafkaConfig) *kafka.Client {
	t.Helper()

	mechanism, err := scram.Mechanism(scram.SHA512,
		kafkaConfig.SASLAdminUser, kafkaConfig.SASLAdminSecret)
	require.NoError(t, err,
		"building the SCRAM-SHA-512 mechanism for the administrative principal")

	transport := &kafka.Transport{SASL: mechanism}
	if kafkaConfig.TLS.Enabled {
		transport.TLS = &tls.Config{
			MinVersion: tls.VersionTLS12,
			ServerName: kafkaConfig.TLS.ServerName,
		}
	}
	t.Cleanup(transport.CloseIdleConnections)

	return &kafka.Client{
		Addr:      kafka.TCP(kafkaConfig.Brokers[0]),
		Timeout:   eventsReplayBrokerTimeout,
		Transport: transport,
	}
}

// eventsReplayBrokerTimeout bounds every broker round trip this test makes. Generous enough for
// a cold connection and a SASL handshake, short enough that an unreachable broker fails the test
// rather than hanging it.
const eventsReplayBrokerTimeout = 15 * time.Second

// TestEventsAPI_ASuccessfulReplayAnswers200AndPutsTheEventBackOnItsTopic is the SUCCESS
// path of POST /events/dead-letter/:event_id/replay, executed rather than described.
//
// A 200 cannot be forged from this package.
//
// The registry stays a MOCK, which is what makes the assertions exact.
func TestEventsAPI_ASuccessfulReplayAnswers200AndPutsTheEventBackOnItsTopic(t *testing.T) {
	kafkaConfig, reason, configured := subscribersKafkaEnvironment(t)
	if !configured {
		t.Skip(reason)
	}

	eventID := uuid.NewString()
	row := eventsReplayableRow(eventID)

	apiInstance, datasource := newEventsAPIOverMockDatasource(t, func(cfg *config.Configuration) {
		cfg.Kafka = kafkaConfig
		// Brokers without a sunset date are refused by configuration validation, because a
		// publishing deployment with no usable dual-delivery window is treated as already
		// retired. A future instant keeps the sunset guard transparent.
		cfg.WebhookDeprecationSunsetDate = time.Now().Add(20 * 24 * time.Hour).UTC().Format(time.RFC3339)
	})
	router := apiInstance.Router()

	// RESOLVED THROUGH PRODUCTION, and only after the configuration is published:
	// TopicForEvent reads the prefix from the store, and the topic the fixture names must
	// be one Blnk OWNS under that prefix or the publisher refuses the request for a reason
	// that has nothing to do with this test. Spelling it out here instead would hard-code
	// a prefix the environment is free to change.
	row.Topic = blnk.TopicForEvent(row.EventType)
	row.DLTTopic = blnk.DLTFor(row.Topic)
	require.NotEmpty(t, row.Topic, "the fixture's event type must route to a topic")

	datasource.On("ClaimEventForReplay", mock.Anything, eventID, mock.Anything).Return(row, nil)

	// The acknowledged coordinate is CAPTURED rather than matched, because it is what the
	// broker chose and the test cannot know it in advance. It is also the only place the
	// service's view of the publish is observable from this package.
	var acknowledged coremodel.BrokerRecord
	datasource.On("MarkEventDispatched", mock.Anything, row.ID, row.ClaimToken, mock.Anything, false).
		Return(nil).
		Run(func(args mock.Arguments) {
			record, isRecord := args.Get(3).(coremodel.BrokerRecord)
			require.True(t, isRecord, "the dispatch transition must be given a broker record")
			acknowledged = record
		})
	// Not expected to be reached: a successful replay marks the row dispatched and
	// releases nothing. Programmed so that a release would be RECORDED rather than
	// panicking the mock, which is what lets the assertion below name it.
	datasource.On("ReleaseEventReplay", mock.Anything, row.ID, row.ClaimToken, mock.Anything).
		Return(nil).Maybe()

	before := time.Now().UTC()
	recorder := eventsKeyedRequest(t, router,
		http.MethodPost, "/events/dead-letter/"+eventID+"/replay", eventsMockMasterKey)
	after := time.Now().UTC()

	// 1. THE EXACT STATUS. 200 and nothing else: a replay returns a body, so 204 would be
	// wrong, and it creates no addressable resource, so 201 would be too.
	require.Equalf(t, http.StatusOK, recorder.Code,
		"a successful replay answers 200. body: %s", recorder.Body.String())

	// 2. THE EXACT BODY, decoded into the declared DTO and then read again as raw keys, so a
	// field renamed in the response but not in the model cannot pass.
	var response model.ReplayEventResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &response),
		"the success body must decode as the declared response: %s", recorder.Body.String())

	assert.Equal(t, eventID, response.EventID,
		"THE EVENT ID IS UNCHANGED BY A REPLAY. That is what lets a subscriber deduplicating on "+
			"event_id absorb the copy, and a handler returning a fresh id would break exactly that")
	assert.Equal(t, row.Topic, response.Topic,
		"the ORIGINAL category topic the event went back to, never the dead-letter topic it was "+
			"listed from")
	assert.NotContains(t, response.Topic, blnk.DeadLetterTopicSuffix,
		"a client told to reason about the .dlt topic would consume from the wrong place")
	assert.Equal(t, string(coremodel.PublishStatusDispatched), response.Status,
		"the broker acknowledged the record, so the reported status is dispatched rather than "+
			"retrying or dead-lettered")

	require.False(t, response.ReplayedAt.IsZero(),
		"the acknowledgement instant must be reported: it is what an operator correlates the "+
			"replay against")
	assert.Falsef(t, response.ReplayedAt.Before(before.Add(-time.Second)) ||
		response.ReplayedAt.After(after.Add(time.Second)),
		"replayed_at must be the instant of THIS replay, not a stored one: got %s, request ran "+
			"between %s and %s", response.ReplayedAt, before, after)

	var rawBody map[string]interface{}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &rawBody))
	assert.Equal(t, eventID, rawBody["event_id"], "the wire key is event_id")
	assert.Equal(t, row.Topic, rawBody["topic"], "the wire key is topic")
	assert.NotContains(t, recorder.Body.String(), "failure",
		"failure metadata has no place on a success")

	// 3. THE BOOKKEEPING RAN, and the coordinate it recorded is a real one.
	// The final argument is FALSE, and the assertion names it rather than accepting anything: a
	// replay must not settle the legacy webhook leg. A row dead-lettered before its enqueue
	// succeeded still owes one, and only the repair leg finishes it.
	datasource.AssertCalled(t, "MarkEventDispatched",
		mock.Anything, row.ID, row.ClaimToken, mock.Anything, false)
	datasource.AssertNotCalled(t, "ReleaseEventReplay",
		mock.Anything, row.ID, row.ClaimToken, mock.Anything)

	require.True(t, acknowledged.Confirmed(),
		"the broker must have acknowledged the record with a coordinate; an unconfirmed record "+
			"would leave the row claiming a publication it cannot name")
	require.Equal(t, row.Topic, acknowledged.Topic,
		"the acknowledged coordinate must name the original topic")
	require.GreaterOrEqual(t, acknowledged.Offset, int64(0),
		"a published record occupies a non-negative offset")

	// 4. THE RECORD IS ON THE TOPIC, read at exactly the coordinate the broker named.
	client := eventsReplayKafkaClient(t, kafkaConfig)

	ctx, cancel := context.WithTimeout(context.Background(), eventsReplayBrokerTimeout)
	defer cancel()

	fetched, err := client.Fetch(ctx, &kafka.FetchRequest{
		Topic:     acknowledged.Topic,
		Partition: acknowledged.Partition,
		Offset:    acknowledged.Offset,
		MinBytes:  1,
		MaxBytes:  1 << 20,
		MaxWait:   eventsReplayBrokerTimeout / 3,
	})
	require.NoError(t, err, "fetching %s/%d at offset %d",
		acknowledged.Topic, acknowledged.Partition, acknowledged.Offset)
	require.NoError(t, fetched.Error, "the broker refused the fetch")
	require.NotNil(t, fetched.Records)

	record, err := fetched.Records.ReadRecord()
	require.NoErrorf(t, err,
		"NO RECORD AT %s/%d OFFSET %d. The response reported a successful replay and the "+
			"bookkeeping recorded that coordinate, so a record has to be there",
		acknowledged.Topic, acknowledged.Partition, acknowledged.Offset)
	require.Equal(t, acknowledged.Offset, record.Offset,
		"the record read must be the one at the acknowledged offset")

	key, err := kafka.ReadAll(record.Key)
	require.NoError(t, err, "reading the replayed record's key")
	assert.Equal(t, row.EffectiveKey(), string(key),
		"the replay must be keyed as the original was, or it lands on a different partition and "+
			"the aggregate's ordering is broken by the very act of repairing it")

	value, err := kafka.ReadAll(record.Value)
	require.NoError(t, err, "reading the replayed record's value")
	assert.Equal(t, string(row.EventRaw), string(value),
		"A REPLAY REPUBLISHES THE STORED BYTES. Re-marshalling from a struct would produce a "+
			"value that differs from the original in field order or in a zero value, and the "+
			"byte-fidelity criterion is what a subscriber's deduplication depends on")

	datasource.AssertExpectations(t)
}
