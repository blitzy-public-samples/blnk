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
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// This file covers the SUBSCRIBER STREAM GATEWAY, which is the enforcement point for a
// subscriber's partition-key prefix.
//
// # Why these assertions are the security-critical ones in the event pipeline
//
// Kafka's authorizer has no message-key resource type, so a subscriber confined to a key
// prefix cannot be given a broker grant that expresses it. The pipeline's answer is a pair:
// such a subscriber is granted Describe and NO Read — which event_isolation_integration_test.go
// proves against a real broker — and its records are served by this gateway, which applies the
// prefix per record.
//
// That makes this code the WHOLE of the boundary on the path that remains. A bug here does not
// degrade a feature: it hands one subscriber another ledger's events. So the tests below assert
// what is NOT returned as carefully as what is, and they assert it against the real
// FetchTopicRecords reader rather than a stub, because "the filter ran" and "the filter ran on
// the records the broker actually sent" are different claims.

// streamFixtureSecret is the secret subscriberProvisionedRow derives its stored credential
// reference from. It is the correct password for that fixture and is used verbatim, because the
// gateway's authentication is a re-derivation: a different string here would be a different
// credential, which is what the negative cases use.
const streamFixtureSecret = "a-secret-that-was-returned-once"

// streamFixtureTopic is a real grantable category topic under the default prefix.
//
// A REAL one, not an invented name, because the gateway refuses a topic no subscriber may be
// granted before it consults the row's own grant — so a fixture topic outside the catalogue
// would make every test below pass for the wrong reason.
const streamFixtureTopic = "blnk.transactions"

// streamRun is one prepared gateway: a registry holding a subscriber, a fake broker holding a
// log, and the gateway wired to both through the REAL admin client.
type streamRun struct {
	store   *subscriberTestStore
	fake    *fakeAdminClient
	admin   *KafkaAdminClient
	gateway *SubscriberStreamGateway
}

// newStreamRun builds a run around one registry row.
//
// The admin client is the PRODUCTION one, pointed at the fake transport. That is deliberate:
// the gateway's contract with the broker — the bounds it sends, the reader it drains, the
// broker error it surfaces rather than reads past — is as much a part of the boundary as the
// key comparison, and a hand-written fetcher stub would assert none of it.
func newStreamRun(t *testing.T, row model.EventSubscriber) *streamRun {
	t.Helper()

	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	store := newSubscriberTestStore(newSubscriberCallLog()).with(row)
	fake := newFakeAdminClient().withTopic(streamFixtureTopic, MinTopicPartitions)
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	return &streamRun{
		store:   store,
		fake:    fake,
		admin:   admin,
		gateway: NewSubscriberStreamGateway(store, admin),
	}
}

// streamRequest builds a well-formed request for the fixture subscriber, which the negative
// tests then spoil one field at a time.
func streamRequest() SubscriberStreamRequest {
	return SubscriberStreamRequest{
		SubscriberID: subscriberFixtureID,
		Secret:       streamFixtureSecret,
		Topic:        streamFixtureTopic,
		Partition:    0,
		Offset:       0,
	}
}

// streamKeyScopedRow is a provisioned subscriber confined to one ledger's records.
func streamKeyScopedRow(t *testing.T, prefix string) model.EventSubscriber {
	t.Helper()

	row := subscriberProvisionedRow(t)
	row.PartitionKeyPrefix = &prefix

	return row
}

// ---------------------------------------------------------------------------------------
// SEC-KEY-01 — the boundary itself
// ---------------------------------------------------------------------------------------

// TestSubscriberStreamGateway_WithholdsEveryRecordOutsideTheKeyScope is the assertion the
// CRITICAL finding turns on.
//
// # What it proves
//
// A partition carrying records for three ledgers is served to a subscriber scoped to one, and
// ONLY that ledger's records come back. The others are not redacted, not summarised and not
// present in any field a caller could reconstruct them from — their existence is reflected in
// one number.
//
// # Why the interleaving matters
//
// The out-of-scope records are placed BEFORE, BETWEEN and AFTER the in-scope ones. A filter
// that stopped at the first refusal, or that only checked the first record of a page, would
// pass a fixture where the entitled records came first — and both are plausible bugs, because
// both look like an early return.
func TestSubscriberStreamGateway_WithholdsEveryRecordOutsideTheKeyScope(t *testing.T) {
	run := newStreamRun(t, streamKeyScopedRow(t, "ldg_acme"))

	run.fake.withRecords(streamFixtureTopic, 0, 0,
		[2]string{"ldg_umbrella", `{"event_id":"a","aggregate_id":"ldg_umbrella"}`},
		[2]string{"ldg_acme", `{"event_id":"b","aggregate_id":"ldg_acme"}`},
		[2]string{"ldg_initech", `{"event_id":"c","aggregate_id":"ldg_initech"}`},
		[2]string{"ldg_acme_settlement", `{"event_id":"d","aggregate_id":"ldg_acme_settlement"}`},
		[2]string{"ldg_umbrella", `{"event_id":"e","aggregate_id":"ldg_umbrella"}`},
	)

	page, err := run.gateway.ReadEvents(context.Background(), streamRequest())
	require.NoError(t, err)

	keys := make([]string, 0, len(page.Records))
	for _, record := range page.Records {
		keys = append(keys, record.Key)
	}

	// A PREFIX TEST, not an equality test, and the fixture proves which: ldg_acme_settlement
	// carries the prefix and is therefore in scope. That is the recorded rule —
	// model.EventSubscriber.HasKeyAccess is a byte-exact strings.HasPrefix — and a gateway that
	// tightened it to equality would silently withhold records the registry entitles the
	// subscriber to.
	assert.Equal(t, []string{"ldg_acme", "ldg_acme_settlement"}, keys,
		"ONLY the records whose key carries the subscriber's prefix may be returned; every other "+
			"key on this shared topic belongs to another ledger")

	assert.Equal(t, 5, page.RecordsScanned, "all five were read")
	assert.Equal(t, 3, page.RecordsWithheld,
		"and three were withheld, which is the only trace a withheld record leaves")

	// NOTHING ABOUT A WITHHELD RECORD MAY SURVIVE. Asserted over the whole page rather than
	// over the record slice, because the leak this guards against is a diagnostic field — a
	// "skipped_keys" list, a last-scanned-key marker — added later in good faith.
	rendered := fmt.Sprintf("%+v", page)
	for _, foreign := range []string{"ldg_umbrella", "ldg_initech"} {
		assert.NotContains(t, rendered, foreign,
			"no key outside the scope may appear anywhere in the page, not even as diagnostics")
	}

	assert.Equal(t, "ldg_acme", page.KeyScope, "the page states the scope it applied")
	assert.True(t, page.KeyScopeEnforced, "and that it WAS applied, which is the claim under test")
}

// TestSubscriberStreamGateway_AdvancesThePastWithheldRecords is the availability half of the
// same boundary, and it is why the cursor is derived from what was SCANNED.
//
// A key-scoped subscriber whose entitled records sit behind a run of records belonging to other
// ledgers would never reach them if next_offset only moved for records it received: every poll
// would re-read the same foreign records and return the same empty page forever. The filter
// would be correct and the subscriber would be permanently starved.
func TestSubscriberStreamGateway_AdvancesThePastWithheldRecords(t *testing.T) {
	run := newStreamRun(t, streamKeyScopedRow(t, "ldg_acme"))

	run.fake.withRecords(streamFixtureTopic, 0, 0,
		[2]string{"ldg_umbrella", `{"event_id":"a"}`},
		[2]string{"ldg_initech", `{"event_id":"b"}`},
		[2]string{"ldg_umbrella", `{"event_id":"c"}`},
	)

	page, err := run.gateway.ReadEvents(context.Background(), streamRequest())
	require.NoError(t, err)

	assert.Empty(t, page.Records, "none of these records belongs to this subscriber")
	assert.Equal(t, 3, page.RecordsWithheld)
	assert.Equal(t, int64(3), page.NextOffset,
		"THE CURSOR MUST STILL ADVANCE. A page of entirely withheld records that left the offset "+
			"where it was would make the next poll re-read the same records and return the same "+
			"empty page, forever — a correct filter that starves the subscriber it protects")

	assert.Equal(t, int64(3), page.HighWatermark,
		"and the watermark is reported, so a client can see it is caught up rather than blocked")
}

// TestSubscriberStreamGateway_DeliversEveryRecordToAnUnscopedSubscriber is the control.
//
// A subscriber with no prefix recorded is entitled to every record on the topics it was granted
// — that is what a whole-topic grant means, and such a subscriber can read the topic directly
// from the broker in any case. A gateway that filtered it anyway would be inventing a boundary
// nobody recorded, and the two paths would return different streams for the same credential.
func TestSubscriberStreamGateway_DeliversEveryRecordToAnUnscopedSubscriber(t *testing.T) {
	run := newStreamRun(t, subscriberProvisionedRow(t))

	run.fake.withRecords(streamFixtureTopic, 0, 0,
		[2]string{"ldg_umbrella", `{"event_id":"a"}`},
		[2]string{"ldg_acme", `{"event_id":"b"}`},
		[2]string{"", `{"event_id":"c"}`},
	)

	page, err := run.gateway.ReadEvents(context.Background(), streamRequest())
	require.NoError(t, err)

	require.Len(t, page.Records, 3,
		"a subscriber that recorded no prefix is entitled to the whole topic, including a record "+
			"with no key at all")
	assert.Zero(t, page.RecordsWithheld)
	assert.Empty(t, page.KeyScope)
	assert.False(t, page.KeyScopeEnforced,
		"and the page says no key boundary was applied, rather than implying one was")
}

// TestSubscriberStreamGateway_WithholdsAKeylessRecordFromAKeyScopedSubscriber pins the one
// record shape whose treatment is not obvious.
//
// A record with no key carries no prefix, so it is OUTSIDE every non-empty scope. Delivering it
// — on the reasoning that an absent key cannot belong to another ledger either — would hand a
// key-scoped subscriber records it was not entitled to whenever anything on a shared topic
// published without a key.
func TestSubscriberStreamGateway_WithholdsAKeylessRecordFromAKeyScopedSubscriber(t *testing.T) {
	run := newStreamRun(t, streamKeyScopedRow(t, "ldg_acme"))

	run.fake.withRecords(streamFixtureTopic, 0, 0,
		[2]string{"", `{"event_id":"keyless"}`},
		[2]string{"ldg_acme", `{"event_id":"mine"}`},
	)

	page, err := run.gateway.ReadEvents(context.Background(), streamRequest())
	require.NoError(t, err)

	require.Len(t, page.Records, 1)
	assert.Equal(t, "ldg_acme", page.Records[0].Key)
	assert.Equal(t, 1, page.RecordsWithheld,
		"an empty key carries no prefix, so it falls outside a non-empty scope; the alternative "+
			"reading delivers every unkeyed record on a shared topic to every scoped subscriber")
}

// TestSubscriberStreamGateway_ReturnsTheEnvelopeBytesVerbatim is the payload-fidelity claim.
//
// The gateway is an alternative TRANSPORT for the same records, so a subscriber reading through
// it and a subscriber consuming the topic directly must parse identical bytes. Re-marshalling
// would reorder keys and rewrite numbers, and the two paths would disagree about one event.
func TestSubscriberStreamGateway_ReturnsTheEnvelopeBytesVerbatim(t *testing.T) {
	run := newStreamRun(t, subscriberProvisionedRow(t))

	// Deliberately awkward JSON: keys out of alphabetical order, a large integer that a
	// float64 round trip would corrupt, and whitespace. All three survive a byte copy and none
	// survives a decode-and-re-encode.
	envelope := `{"schema_version":1,"event_id":"9f1c","payload":{"amount":9007199254740993}, "event_type":"transaction.applied"}`
	run.fake.withRecords(streamFixtureTopic, 0, 0, [2]string{"ldg_acme", envelope})

	page, err := run.gateway.ReadEvents(context.Background(), streamRequest())
	require.NoError(t, err)

	require.Len(t, page.Records, 1)
	assert.Equal(t, envelope, string(page.Records[0].Value),
		"the envelope must be returned BYTE FOR BYTE. A re-marshal would reorder the keys and turn "+
			"9007199254740993 into 9007199254740992, so a subscriber comparing this against the same "+
			"record read from the topic would find two different events")
}

// ---------------------------------------------------------------------------------------
// Authentication
// ---------------------------------------------------------------------------------------

// TestSubscriberStreamGateway_RefusesEveryUnauthenticatedShapeIdentically is the enumeration
// guard.
//
// # Why one answer for five causes
//
// This is the only endpoint in the subscriber surface a non-operator reaches. A distinct answer
// per cause would make it an oracle over the registry: "does subscriber acme-prod exist?" is
// answered by the difference between 404 and 401, and subscriber identifiers are
// tenant-chosen names. So every failure returns SUBSCRIBER_CREDENTIAL_INVALID with the same
// message, and the reason is recorded in a log line the caller never sees.
//
// The five shapes are asserted together, in one table, because the property is that they are
// INDISTINGUISHABLE — a test per shape would assert each one's code and never notice that two
// of them had drifted apart.
func TestSubscriberStreamGateway_RefusesEveryUnauthenticatedShapeIdentically(t *testing.T) {
	revoked := subscriberProvisionedRow(t)
	revocationPending := time.Now().UTC().Add(-time.Minute)
	revoked.RevocationPendingAt = &revocationPending

	unprovisioned := subscriberFixtureRow(t)

	cases := map[string]struct {
		row     model.EventSubscriber
		mutate  func(*SubscriberStreamRequest)
		because string
	}{
		"no such subscriber": {
			row:     subscriberProvisionedRow(t),
			mutate:  func(r *SubscriberStreamRequest) { r.SubscriberID = "sub_does_not_exist" },
			because: "a 404 here would answer 'does this subscriber exist' to anyone who asks",
		},
		"the subscriber holds no credential": {
			row:     unprovisioned,
			mutate:  func(*SubscriberStreamRequest) {},
			because: "and a distinct answer would say 'this one exists but has no credential yet'",
		},
		"the wrong secret": {
			row:     subscriberProvisionedRow(t),
			mutate:  func(r *SubscriberStreamRequest) { r.Secret = "not-the-secret" },
			because: "the ordinary failure, and the one the others must be indistinguishable from",
		},
		"no secret at all": {
			row:     subscriberProvisionedRow(t),
			mutate:  func(r *SubscriberStreamRequest) { r.Secret = "   " },
			because: "an absent credential is a failed authentication, not a malformed request",
		},
		"a foreign principal": {
			row:     subscriberProvisionedRow(t),
			mutate:  func(r *SubscriberStreamRequest) { r.Principal = "blnk-sub-somebody-else" },
			because: "a caller naming another principal is asking about another identity",
		},
		"a credential pending revocation": {
			row:    revoked,
			mutate: func(*SubscriberStreamRequest) {},
			because: "an operator has asked for this access to end; serving it until the broker " +
				"confirms would keep open the one door that was asked to be closed",
		},
	}

	var messages []string
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			run := newStreamRun(t, tc.row)
			run.fake.withRecords(streamFixtureTopic, 0, 0,
				[2]string{"ldg_acme", `{"event_id":"must-not-be-returned"}`})

			request := streamRequest()
			tc.mutate(&request)

			page, err := run.gateway.ReadEvents(context.Background(), request)

			requireSubscriberAPIError(t, err, apierror.ErrSubscriberCredentialInvalid)
			assert.Equal(t, 401, apierror.StatusForCode(apierror.ErrSubscriberCredentialInvalid),
				"401 and not 403: the caller has not established WHO it is, which another secret "+
					"could fix — unlike a topic outside the grant, which none can")
			assert.Empty(t, page.Records, "%s", tc.because)

			assert.Zero(t, run.fake.callCount("Fetch"),
				"THE BROKER MUST NOT BE TOUCHED. A gateway that fetched before authenticating would "+
					"read records into this process on behalf of a caller that proved nothing")

			var apiErr apierror.APIError
			require.ErrorAs(t, err, &apiErr)
			messages = append(messages, apiErr.Message)
		})
	}

	require.Len(t, messages, len(cases), "every shape must have contributed a message")
	for _, message := range messages {
		assert.Equal(t, messages[0], message,
			"every authentication failure must return the IDENTICAL message; a difference between "+
				"any two of them is an enumeration oracle over tenant-chosen subscriber names")
	}
}

// TestSubscriberStreamGateway_RecordsTheRefusalReasonWhereOnlyAnOperatorSeesIt is the other
// half of that split.
//
// Refusing identically is only acceptable if the cause is recorded SOMEWHERE, or an operator
// debugging a subscriber that cannot connect has nothing to read. The log line is that place,
// and the two properties are tested together because either alone is a defect: an informative
// error body is an oracle, and an uninformative log is an unsupportable system.
func TestSubscriberStreamGateway_RecordsTheRefusalReasonWhereOnlyAnOperatorSeesIt(t *testing.T) {
	run := newStreamRun(t, subscriberProvisionedRow(t))

	hook := logtest.NewGlobal()
	defer hook.Reset()

	request := streamRequest()
	request.Secret = "not-the-secret"

	_, err := run.gateway.ReadEvents(context.Background(), request)
	require.Error(t, err)

	var entry *logrus.Entry
	for index := range hook.Entries {
		if strings.Contains(hook.Entries[index].Message, "could not be authenticated") {
			entry = &hook.Entries[index]

			break
		}
	}

	require.NotNil(t, entry, "the refusal must be recorded, or its cause exists nowhere at all")
	assert.Equal(t, logrus.WarnLevel, entry.Level)
	assert.Equal(t, "the presented secret does not match the recorded credential", entry.Data["reason"],
		"the log carries the cause the caller is deliberately not told")

	// HASHED, like every other subscriber identifier in a log line: the id is a tenant-chosen
	// name, and this line is emitted on unauthenticated traffic, so it is the easiest line in
	// the system for an attacker to fill with names of its choosing.
	assert.NotContains(t, entry.Data, "subscriber_id")
	assert.Equal(t, subscriberLogLabel(subscriberFixtureID), entry.Data["subscriber_id_hash"])

	// AND THE SECRET APPEARS NOWHERE. Asserted over the whole entry and the error, because a
	// field added later "for debugging" is exactly how a password reaches a log aggregator.
	rendered := fmt.Sprintf("%+v %v", entry.Data, err)
	assert.NotContains(t, rendered, "not-the-secret")
	assert.NotContains(t, rendered, streamFixtureSecret)
}

// TestSubscriberStreamGateway_AcceptsTheMatchingPrincipal is the positive counterpart to the
// foreign-principal refusal, and it is what stops that check being satisfied by refusing every
// principal.
func TestSubscriberStreamGateway_AcceptsTheMatchingPrincipal(t *testing.T) {
	row := subscriberProvisionedRow(t)
	run := newStreamRun(t, row)
	run.fake.withRecords(streamFixtureTopic, 0, 0, [2]string{"ldg_acme", `{"event_id":"a"}`})

	request := streamRequest()
	request.Principal = row.KafkaPrincipal

	page, err := run.gateway.ReadEvents(context.Background(), request)
	require.NoError(t, err, "the principal the registry recorded must be accepted when sent")
	assert.Len(t, page.Records, 1)
}

// ---------------------------------------------------------------------------------------
// Topic authorization
// ---------------------------------------------------------------------------------------

// TestSubscriberStreamGateway_RefusesATopicOutsideTheGrant is the topic dimension, enforced
// here because on this path nothing else can enforce it.
//
// The gateway reads with BLNK's credential, which is wide enough for every category topic. The
// broker will therefore serve any topic this process asks for, so the subscriber's own grant —
// which Kafka does enforce on the direct path — has no enforcement on this one unless the
// gateway provides it. Omitting the check would make this endpoint a hole in a boundary the
// rest of the system keeps.
func TestSubscriberStreamGateway_RefusesATopicOutsideTheGrant(t *testing.T) {
	row := subscriberProvisionedRow(t)
	row.AuthorizedTopics = []string{streamFixtureTopic}

	for name, topic := range map[string]string{
		"another category topic":     "blnk.balances",
		"a dead-letter topic":        "blnk.transactions.dlt",
		"the internal system topic":  "blnk.system",
		"a topic outside the prefix": "someone.else.transactions",
		"an invented topic":          "blnk.not-a-category",
	} {
		t.Run(name, func(t *testing.T) {
			run := newStreamRun(t, row)
			run.fake.
				withTopic(topic, MinTopicPartitions).
				withRecords(topic, 0, 0, [2]string{"ldg_acme", `{"event_id":"must-not-be-returned"}`})

			request := streamRequest()
			request.Topic = topic

			page, err := run.gateway.ReadEvents(context.Background(), request)

			requireSubscriberAPIError(t, err, apierror.ErrSubscriberTopicNotGranted)
			assert.Equal(t, 403, apierror.StatusForCode(apierror.ErrSubscriberTopicNotGranted),
				"403 and not 401: the caller is known and no credential can change the answer, so a "+
					"401 would invite a re-authentication loop against an authorization decision")
			assert.Empty(t, page.Records)

			assert.Zero(t, run.fake.callCount("Fetch"),
				"and the broker is not touched: authorization precedes the read, so an unauthorised "+
					"topic's records never enter this process")
		})
	}
}

// TestSubscriberStreamGateway_RefusesADeadLetterTopicEvenWhenTheRowNamesIt is the guard that
// does not depend on registration having done its job.
//
// Registration refuses to grant a dead-letter topic, so a row naming one should not exist. This
// asserts the gateway refuses it anyway, because "should not exist" is a claim about another
// component: a row written before that refusal, or by a direct SQL edit, would otherwise hand a
// subscriber Blnk's own failure stream — every event that could not be delivered, for every
// tenant.
func TestSubscriberStreamGateway_RefusesADeadLetterTopicEvenWhenTheRowNamesIt(t *testing.T) {
	dlt := DLTFor(streamFixtureTopic)

	row := subscriberProvisionedRow(t)
	row.AuthorizedTopics = []string{streamFixtureTopic, dlt}

	run := newStreamRun(t, row)
	run.fake.
		withTopic(dlt, MinTopicPartitions).
		withRecords(dlt, 0, 0, [2]string{"ldg_acme", `{"event_id":"someone-elses-failure"}`})

	request := streamRequest()
	request.Topic = dlt

	page, err := run.gateway.ReadEvents(context.Background(), request)

	requireSubscriberAPIError(t, err, apierror.ErrSubscriberTopicNotGranted)
	assert.Empty(t, page.Records,
		"the grantability rule is read from the same place registration reads it, so a row that "+
			"names an ungrantable topic is refused here rather than trusted")
}

// TestSubscriberStreamGateway_RefusesARequestNamingNoTopic keeps the missing-parameter case a
// 400 rather than letting it fall through to a topic-not-granted 403, which would tell a
// caller its grant was wrong when its request was.
func TestSubscriberStreamGateway_RefusesARequestNamingNoTopic(t *testing.T) {
	run := newStreamRun(t, subscriberProvisionedRow(t))

	request := streamRequest()
	request.Topic = "  "

	_, err := run.gateway.ReadEvents(context.Background(), request)
	requireSubscriberAPIError(t, err, apierror.ErrGenMissingParameter)
}

// ---------------------------------------------------------------------------------------
// Bounds, cursor and degradation
// ---------------------------------------------------------------------------------------

// TestSubscriberStreamGateway_BoundsEveryReadItSends asserts the bounds that protect this
// process, on the request the broker actually received.
//
// An unbounded read of a shared category topic has a cost decided by however many records
// happen to be on the partition, which is not a property any caller can know. So the bounds are
// asserted on the outgoing FetchRequest rather than inferred from the page that came back: a
// gateway that clamped its own page after asking the broker for everything would look correct
// from the response and would still have read the whole partition into memory.
func TestSubscriberStreamGateway_BoundsEveryReadItSends(t *testing.T) {
	t.Run("a request naming no bounds gets the documented defaults", func(t *testing.T) {
		run := newStreamRun(t, subscriberProvisionedRow(t))

		_, err := run.gateway.ReadEvents(context.Background(), streamRequest())
		require.NoError(t, err)

		sent := run.fake.fetchBounds()
		require.Len(t, sent, 1)
		assert.Equal(t, DefaultSubscriberStreamMaxWait, sent[0].MaxWait)
		assert.Equal(t, MaxSubscriberStreamBytes, int64(sent[0].MaxBytes),
			"the BYTE bound is what actually protects memory: one oversized record exceeds any "+
				"record count")
		assert.Equal(t, int64(1), sent[0].MinBytes,
			"one byte, so max_wait is a ceiling rather than a floor and a quiet partition answers "+
				"immediately")
		assert.Equal(t, kafka.ReadCommitted, sent[0].IsolationLevel,
			"read_committed on a subscriber-facing read: it can never disclose an aborted record")
	})

	t.Run("an oversized page size is clamped rather than refused", func(t *testing.T) {
		run := newStreamRun(t, subscriberProvisionedRow(t))

		pairs := make([][2]string, 0, MaxSubscriberStreamLimit+10)
		for index := 0; index < MaxSubscriberStreamLimit+10; index++ {
			pairs = append(pairs, [2]string{"ldg_acme", fmt.Sprintf(`{"event_id":"%d"}`, index)})
		}
		run.fake.withRecords(streamFixtureTopic, 0, 0, pairs...)

		request := streamRequest()
		request.Limit = MaxSubscriberStreamLimit * 10

		page, err := run.gateway.ReadEvents(context.Background(), request)
		require.NoError(t, err,
			"a caller asking for a larger page is expressing a throughput preference, not making "+
				"an error, and the response reports what was actually read")

		assert.Len(t, page.Records, MaxSubscriberStreamLimit)
		assert.True(t, page.Truncated,
			"and TRUNCATED says more is immediately available, so a client does not wait a poll "+
				"interval for records already on the partition")
		assert.Equal(t, int64(MaxSubscriberStreamLimit), page.NextOffset)
	})

	t.Run("an oversized wait is clamped", func(t *testing.T) {
		run := newStreamRun(t, subscriberProvisionedRow(t))

		request := streamRequest()
		request.MaxWait = time.Hour

		_, err := run.gateway.ReadEvents(context.Background(), request)
		require.NoError(t, err)

		sent := run.fake.fetchBounds()
		require.Len(t, sent, 1)
		assert.Equal(t, MaxSubscriberStreamMaxWait, sent[0].MaxWait,
			"an hour-long hold would let a client convert long-polling into connection exhaustion")
	})

	t.Run("the budget exceeds the longest wait it must contain", func(t *testing.T) {
		// Otherwise a caller asking for the maximum wait would have its own request expire
		// before the broker answered — a timeout produced entirely by Blnk's own two constants
		// disagreeing.
		assert.Greater(t, SubscriberStreamBudget, MaxSubscriberStreamMaxWait,
			"the request budget must leave room for the registry read and the fetch on top of the "+
				"longest hold a caller may ask the broker for")
	})
}

// TestSubscriberStreamGateway_RefusesAnUnrecognisedOffset draws the line between the bounds
// that are clamped and the cursor, which is not.
//
// A page size out of range is a preference. An offset out of range is a client defect, and
// silently substituting one would serve the wrong records rather than fewer of them —
// duplicate processing at best, skipped events at worst, and nothing in the response to say so.
func TestSubscriberStreamGateway_RefusesAnUnrecognisedOffset(t *testing.T) {
	run := newStreamRun(t, subscriberProvisionedRow(t))

	for name, offset := range map[string]int64{
		"a negative offset that is not a sentinel": -7,
		"far below the sentinels":                  -1000,
	} {
		t.Run(name, func(t *testing.T) {
			request := streamRequest()
			request.Offset = offset

			_, err := run.gateway.ReadEvents(context.Background(), request)
			requireSubscriberAPIError(t, err, apierror.ErrGenValidation)
		})
	}

	t.Run("a negative partition is refused too", func(t *testing.T) {
		request := streamRequest()
		request.Partition = -1

		_, err := run.gateway.ReadEvents(context.Background(), request)
		requireSubscriberAPIError(t, err, apierror.ErrGenValidation)
	})

	for name, offset := range map[string]int64{
		"the earliest sentinel": SubscriberStreamOffsetEarliest,
		"the latest sentinel":   SubscriberStreamOffsetLatest,
	} {
		t.Run(name+" is honoured", func(t *testing.T) {
			// The two Kafka sentinels are the protocol's own values rather than a Blnk
			// invention, so a client that already speaks Kafka sends the number it knows.
			fresh := newStreamRun(t, subscriberProvisionedRow(t))
			fresh.fake.withRecords(streamFixtureTopic, 0, 0,
				[2]string{"ldg_acme", `{"event_id":"a"}`},
				[2]string{"ldg_acme", `{"event_id":"b"}`},
			)

			request := streamRequest()
			request.Offset = offset

			page, err := fresh.gateway.ReadEvents(context.Background(), request)
			require.NoError(t, err)

			if offset == SubscriberStreamOffsetEarliest {
				assert.Len(t, page.Records, 2, "the earliest sentinel reads the retained log")
				assert.Equal(t, int64(2), page.NextOffset)

				return
			}

			assert.Empty(t, page.Records, "the latest sentinel reads nothing now")
			assert.Equal(t, page.HighWatermark, page.NextOffset,
				"and answers with the end of the log, which is where the caller resumes — the "+
					"requested value is a sentinel, so echoing it back would be uninterpretable")
		})
	}
}

// TestSubscriberStreamGateway_ReportsAnUnavailableStreamRatherThanAnEmptyPage covers the two
// degraded shapes, and the reason they are not answered with 200 and no records.
//
// An empty page means "caught up", and a client that believed it would advance nothing and
// report itself healthy. The distinction matters most in exactly the case that produces it: a
// deployment where Kafka has gone away, or was never configured, and every subscriber is
// silently receiving nothing.
func TestSubscriberStreamGateway_ReportsAnUnavailableStreamRatherThanAnEmptyPage(t *testing.T) {
	t.Run("no broker configured", func(t *testing.T) {
		store := newSubscriberTestStore(newSubscriberCallLog()).with(subscriberProvisionedRow(t))
		storeKafkaTopicPrefix(t, DefaultTopicPrefix)

		// A gateway with NO fetcher, which is what SubscriberStream builds when the deployment
		// has no KAFKA_BROKERS.
		gateway := NewSubscriberStreamGateway(store, nil)

		page, err := gateway.ReadEvents(context.Background(), streamRequest())

		requireSubscriberAPIError(t, err, apierror.ErrKafkaUnavailable)
		assert.Equal(t, 503, apierror.StatusForCode(apierror.ErrKafkaUnavailable))
		assert.Empty(t, page.Records)
	})

	t.Run("the broker refused the read", func(t *testing.T) {
		run := newStreamRun(t, subscriberProvisionedRow(t))
		run.fake.withFetchError(streamFixtureTopic, 0, kafka.TopicAuthorizationFailed)

		page, err := run.gateway.ReadEvents(context.Background(), streamRequest())

		requireSubscriberAPIError(t, err, apierror.ErrKafkaUnavailable)
		assert.Empty(t, page.Records,
			"a refused read must not be reported as a caught-up subscriber; the broker's verdict "+
				"arrives INSIDE the fetch response, so a gateway that read past it would return an "+
				"empty page for a read it was denied")
	})

	t.Run("an unconfigured admin client", func(t *testing.T) {
		store := newSubscriberTestStore(newSubscriberCallLog()).with(subscriberProvisionedRow(t))
		storeKafkaTopicPrefix(t, DefaultTopicPrefix)

		// The concrete client with no brokers, which is the shape a misconfigured process
		// resolves rather than a nil interface.
		gateway := NewSubscriberStreamGateway(store, &KafkaAdminClient{})

		_, err := gateway.ReadEvents(context.Background(), streamRequest())
		requireSubscriberAPIError(t, err, apierror.ErrKafkaUnavailable)
	})
}

// TestSubscriberStreamGateway_ReportsAnUnavailableRegistry covers the nil-dependency shapes,
// because a nil-safe constructor is only useful if the resulting object answers rather than
// panicking.
func TestSubscriberStreamGateway_ReportsAnUnavailableRegistry(t *testing.T) {
	gateway := NewSubscriberStreamGateway(nil, nil)

	_, err := gateway.ReadEvents(context.Background(), streamRequest())
	requireSubscriberAPIError(t, err, apierror.ErrKafkaUnavailable)

	var nilGateway *SubscriberStreamGateway
	_, err = nilGateway.ReadEvents(context.Background(), streamRequest())
	require.Error(t, err, "a nil gateway must answer rather than panic: the API layer resolves one "+
		"per request and a panic there is a 500 on a route that should report 503")
}

// TestSubscriberStreamGateway_PropagatesARegistryFailureAsItself keeps the enumeration guard
// from swallowing real faults.
//
// Flattening a NOT-FOUND into the authentication refusal is the guard. Flattening a dropped
// connection or an expired budget into it would tell a caller its credential was wrong when the
// database was down — sending it to re-issue a credential that works, and hiding an outage.
func TestSubscriberStreamGateway_PropagatesARegistryFailureAsItself(t *testing.T) {
	run := newStreamRun(t, subscriberProvisionedRow(t))
	run.store.failing("GetEventSubscriberByID", errors.New("connection reset by peer"))

	_, err := run.gateway.ReadEvents(context.Background(), streamRequest())

	require.Error(t, err)
	var apiErr apierror.APIError
	if errors.As(err, &apiErr) {
		assert.NotEqual(t, apierror.ErrSubscriberCredentialInvalid, apiErr.Code,
			"a registry outage must not be reported as an invalid credential; the caller would "+
				"re-issue a credential that already works and the outage would go unreported")
	}
}

// TestSubscriberStreamGateway_RefusesAMalformedIdentifierAsAValidationError draws the last line
// in the authentication family.
//
// A value no subscriber id can ever equal is refused as malformed rather than as an
// authentication failure, and that is not an enumeration leak: the answer depends only on the
// shape of the value the caller sent, which it can determine for itself without asking.
func TestSubscriberStreamGateway_RefusesAMalformedIdentifierAsAValidationError(t *testing.T) {
	run := newStreamRun(t, subscriberProvisionedRow(t))

	for _, id := range []string{"*", "sub_ABC!", "", "   "} {
		t.Run(fmt.Sprintf("%q", id), func(t *testing.T) {
			request := streamRequest()
			request.SubscriberID = id

			_, err := run.gateway.ReadEvents(context.Background(), request)
			requireSubscriberAPIError(t, err, apierror.ErrGenValidation)
		})
	}
}

// ---------------------------------------------------------------------------------------
// The reader itself
// ---------------------------------------------------------------------------------------

// TestFetchTopicRecords_RefusesAnUnboundedRead pins the admin client's own guard.
//
// FetchTopicRecords is reachable by any caller in this package, and the gateway is only its
// first. A bound of zero is not "no limit" — it is a caller that forgot one — so the read is
// refused rather than served unbounded.
func TestFetchTopicRecords_RefusesAnUnboundedRead(t *testing.T) {
	fake := newFakeAdminClient().withTopic(streamFixtureTopic, MinTopicPartitions)
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	bounded := TopicRecordFetch{
		Topic:      streamFixtureTopic,
		MaxRecords: 10,
		MaxBytes:   1024,
		MaxWait:    time.Second,
	}

	for name, mutate := range map[string]func(*TopicRecordFetch){
		"no record bound": func(f *TopicRecordFetch) { f.MaxRecords = 0 },
		"no byte bound":   func(f *TopicRecordFetch) { f.MaxBytes = 0 },
		"no wait bound":   func(f *TopicRecordFetch) { f.MaxWait = 0 },
		"no topic":        func(f *TopicRecordFetch) { f.Topic = "  " },
	} {
		t.Run(name, func(t *testing.T) {
			request := bounded
			mutate(&request)

			_, err := admin.FetchTopicRecords(context.Background(), request)
			require.Error(t, err)
			assert.Zero(t, fake.callCount("Fetch"),
				"an unbounded read must be refused before the broker is asked")
		})
	}

	t.Run("a bounded read is served", func(t *testing.T) {
		fake.withRecords(streamFixtureTopic, 0, 0, [2]string{"ldg_acme", `{"event_id":"a"}`})

		batch, err := admin.FetchTopicRecords(context.Background(), bounded)
		require.NoError(t, err)
		require.Len(t, batch.Records, 1,
			"the positive case is what stops the refusals above passing for a broken reader")
		assert.Equal(t, "ldg_acme", string(batch.Records[0].Key))
	})
}

// TestTopicRecordBatch_NextOffsetIsDerivedFromWhatWasScanned states the cursor rule on the
// type that owns it, over the three shapes a page can take.
//
// It is asserted here as well as through the gateway because the rule is the batch's, and a
// caller other than the gateway — a replay, an export — has to be able to rely on it.
func TestTopicRecordBatch_NextOffsetIsDerivedFromWhatWasScanned(t *testing.T) {
	t.Run("records read: one past the last of them", func(t *testing.T) {
		batch := TopicRecordBatch{
			Records:       []FetchedRecord{{Offset: 40}, {Offset: 41}, {Offset: 42}},
			HighWatermark: 99,
		}
		assert.Equal(t, int64(43), batch.NextOffset(40),
			"and it is the LAST record's offset that decides, not the count: a compacted partition "+
				"has gaps, so offset+count would resume before records already read")
	})

	t.Run("nothing read from an absolute offset: the offset stands", func(t *testing.T) {
		batch := TopicRecordBatch{HighWatermark: 99}
		assert.Equal(t, int64(7), batch.NextOffset(7),
			"a caught-up subscriber must resume where it asked, not skip to the watermark — a "+
				"record produced between the fetch and the next poll would otherwise be missed")
	})

	t.Run("nothing read from a sentinel: the watermark", func(t *testing.T) {
		batch := TopicRecordBatch{HighWatermark: 99}
		assert.Equal(t, int64(99), batch.NextOffset(SubscriberStreamOffsetLatest),
			"echoing the sentinel back would hand the caller a cursor it cannot interpret")
	})
}
