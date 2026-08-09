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
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/token"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apimodel "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// event_log_privacy_test.go covers the two disclosure rules the event pipeline's log and trace
// output has to satisfy, and the pivot that makes the second one usable.
//
//   - A KAFKA CAUSE IS CLASSIFIED, NOT RENDERED. A kafka-go error prints with broker
//     hostnames, listener addresses, the topic and partition it was acting on and, for a
//     credential operation, the principal. Logs are shipped to an aggregator and indexed, so
//     the raw text accumulates in a searchable store that a far wider audience reads than the
//     cluster itself.
//   - AN IDENTIFIER IS PSEUDONYMISED. The metric labels already were, for the reason
//     subscriberLagLabel documents; the log fields beside them were not, which both leaked the
//     tenant and made the two UNJOINABLE — an alert naming a hash and a log line naming a name.
//   - THE PSEUDONYM RESOLVES. A hash nobody can turn back into a subscriber is not privacy, it
//     is an outage during an incident, so one identifier must produce ONE token in the metric,
//     the log and the API response, and the registry must resolve it.
//
// The one documented exception is asserted here too rather than left implicit: AAP requirement
// R-4 mandates the error reason on every publish attempt, so PublishResult.LogFields keeps a
// rendered error and this file proves that is still true.

// TestKafkaErrorClass_IsClosedAndSeparatesTheNonFailures pins the log-side Kafka vocabulary.
//
// The two context conditions and not_configured are the cases that decide whether a caller
// warns at all, which is why each is asserted individually rather than as "some class".
func TestKafkaErrorClass_IsClosedAndSeparatesTheNonFailures(t *testing.T) {
	cases := map[string]struct {
		cause error
		want  string
	}{
		"a nil cause is the none class, so a shared line keeps a stable field set": {
			cause: nil, want: kafkaErrorClassNone,
		},
		"an unconfigured broker list is NOT a failure and must be distinguishable": {
			cause: ErrKafkaAdminNotConfigured, want: kafkaErrorClassNotConfigured,
		},
		"a wrapped unconfigured error classifies identically": {
			cause: fmt.Errorf("reading offsets: %w", ErrKafkaAdminNotConfigured),
			want:  kafkaErrorClassNotConfigured,
		},
		"a cancelled caller is not a broker fault": {
			cause: context.Canceled, want: kafkaErrorClassCancelled,
		},
		"an expired budget is not a broker fault either": {
			cause: context.DeadlineExceeded, want: kafkaErrorClassDeadline,
		},
		"a cancellation wrapped by a broker error still classifies as cancellation": {
			cause: fmt.Errorf("write messages: %w", context.Canceled),
			want:  kafkaErrorClassCancelled,
		},
		"a closed publisher is publisher state, not the broker": {
			cause: ErrEventPublisherClosed, want: kafkaErrorClassPublisher,
		},
		"a topic Blnk does not own is a refusal of ours": {
			cause: ErrTopicNotOwned, want: kafkaErrorClassTopicRefused,
		},
		"an oversized message is ours too": {
			cause: ErrEventMessageTooLarge, want: kafkaErrorClassMessageTooBig,
		},
		"a non-enforcing authorizer has its own class, because every isolation guarantee is void": {
			cause: ErrAuthorizerNotEnforcing, want: kafkaErrorClassAuthorizer,
		},
		"a refused partition growth is a geometry problem, not an outage": {
			cause: ErrPartitionGrowthRefused, want: kafkaErrorClassGeometry,
		},
		"an inadequate replication factor is the same class": {
			cause: ErrReplicationFactorInadequate, want: kafkaErrorClassGeometry,
		},
		"anything else is the broker": {
			cause: errors.New("[3] UNKNOWN_TOPIC_OR_PARTITION: broker 10.0.0.7:9092 refused"),
			want:  kafkaErrorClassBroker,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, KafkaErrorClass(tc.cause))
		})
	}
}

// TestKafkaErrorClass_NeverLeaksTheCausesOwnWords is the property the vocabulary exists for.
//
// A class that happened to interpolate the cause would satisfy every mapping assertion above
// and defeat the whole point, so the output is checked for the disclosive substrings rather
// than only for equality with an expected literal.
func TestKafkaErrorClass_NeverLeaksTheCausesOwnWords(t *testing.T) {
	cause := errors.New(
		"failed to dial: dial tcp 10.42.7.19:9093: i/o timeout (principal blnk-sub-acme-payments-eu, " +
			"topic blnk.transactions-42)",
	)

	class := KafkaErrorClass(cause)

	for _, secret := range []string{"10.42.7.19", "9093", "acme-payments-eu", "blnk.transactions-42"} {
		assert.NotContains(t, class, secret,
			"the class is a fixed literal, so no part of the cause may appear in it")
	}
	assert.Equal(t, kafkaErrorClassBroker, class)
}

// TestLogKafkaDiagnostic_EmitsTheRawCauseOnlyAtTrace proves the sink is genuinely separate.
//
// DEBUG is asserted silent, not merely "not INFO": debug is already this pipeline's routine
// per-event volume, so an operator who raises the level to follow a delivery must not thereby
// start shipping cluster topology. That is the whole reason the sink sits one level lower.
func TestLogKafkaDiagnostic_EmitsTheRawCauseOnlyAtTrace(t *testing.T) {
	cause := errors.New("dial tcp 10.42.7.19:9093: i/o timeout")

	t.Run("silent at info", func(t *testing.T) {
		restore := pinLogLevel(t, logrus.InfoLevel)
		defer restore()

		hook := logtest.NewGlobal()
		LogKafkaDiagnostic("publish", cause)

		assert.Empty(t, hook.AllEntries(), "the default level must ship no raw cause at all")
	})

	t.Run("still silent at debug", func(t *testing.T) {
		restore := pinLogLevel(t, logrus.DebugLevel)
		defer restore()

		hook := logtest.NewGlobal()
		LogKafkaDiagnostic("publish", cause)

		assert.Empty(t, hook.AllEntries(),
			"debug is the pipeline's ordinary per-event volume; enabling it must not enable "+
				"disclosure as a side effect")
	})

	t.Run("emitted at trace, carrying the operation", func(t *testing.T) {
		restore := pinLogLevel(t, logrus.TraceLevel)
		defer restore()

		hook := logtest.NewGlobal()
		LogKafkaDiagnostic("read_event_topic_end_offsets", cause)

		entries := hook.AllEntries()
		require.Len(t, entries, 1, "trace is the sink, so exactly one line is expected")
		assert.Equal(t, logrus.TraceLevel, entries[0].Level)
		assert.Equal(t, "read_event_topic_end_offsets", entries[0].Data["operation"],
			"the operation is what correlates the diagnostic with the bounded line beside it")
		assert.Contains(t, fmt.Sprint(entries[0].Data[logrus.ErrorKey]), "10.42.7.19",
			"the sink's whole purpose is that the raw text is reachable when explicitly asked for")
	})

	t.Run("a nil cause is a no-op even at trace", func(t *testing.T) {
		restore := pinLogLevel(t, logrus.TraceLevel)
		defer restore()

		hook := logtest.NewGlobal()
		LogKafkaDiagnostic("publish", nil)

		assert.Empty(t, hook.AllEntries())
	})
}

// TestWithKafkaError_AttachesTheClassAndNotTheCause covers the wrapper every converted call
// site uses.
func TestWithKafkaError_AttachesTheClassAndNotTheCause(t *testing.T) {
	restore := pinLogLevel(t, logrus.InfoLevel)
	defer restore()

	hook := logtest.NewGlobal()
	cause := fmt.Errorf("kafka: %w", ErrAuthorizerNotEnforcing)

	withKafkaError(logrus.WithField("topic", "blnk.transactions"), "ensure_topics", cause).
		Warn("assurance failed")

	entries := hook.AllEntries()
	require.Len(t, entries, 1)
	assert.Equal(t, kafkaErrorClassAuthorizer, entries[0].Data["error_class"])
	assert.Equal(t, "blnk.transactions", entries[0].Data["topic"],
		"the wrapper extends the entry rather than replacing it")
	assert.NotContains(t, entries[0].Data, logrus.ErrorKey,
		"the standard entry must carry no rendered cause")
	assert.NotContains(t, entries[0].Data, "error",
		"and no field named error either, which is the one a scanner would index")
}

// TestKafkaErrorClassField_ReturnsTheClassForAFieldsMap covers the map-literal form.
func TestKafkaErrorClassField_ReturnsTheClassForAFieldsMap(t *testing.T) {
	restore := pinLogLevel(t, logrus.InfoLevel)
	defer restore()

	hook := logtest.NewGlobal()

	logrus.WithFields(logrus.Fields{
		"error_class": kafkaErrorClassField("provision", ErrKafkaAdminNotConfigured),
	}).Warn("provisioning declined")

	entries := hook.AllEntries()
	require.Len(t, entries, 1)
	assert.Equal(t, kafkaErrorClassNotConfigured, entries[0].Data["error_class"])
	assert.Empty(t, hook.AllEntries()[0].Data[logrus.ErrorKey])
}

// TestKafkaErrorEntry_StartsALineCarryingOnlyTheClass covers the third form.
func TestKafkaErrorEntry_StartsALineCarryingOnlyTheClass(t *testing.T) {
	restore := pinLogLevel(t, logrus.InfoLevel)
	defer restore()

	hook := logtest.NewGlobal()
	kafkaErrorEntry("new_kafka_admin", errors.New("open /run/secrets/kafka/ca.pem: no such file")).
		Error("the administrative client could not be built")

	entries := hook.AllEntries()
	require.Len(t, entries, 1)
	assert.Equal(t, kafkaErrorClassBroker, entries[0].Data["error_class"])
	for _, field := range entries[0].Data {
		assert.NotContains(t, fmt.Sprint(field), "/run/secrets",
			"a container path is exactly the disclosure this replaces")
	}
}

// TestSubscriberLogLabel_IsTheSameTokenTheMetricLabelPublishes is the PIVOT INVARIANT.
//
// If these two ever diverge for a registry subscriber, a lag alert names one token and the log
// line explaining it names another, and nothing joins them. That is not a cosmetic difference:
// it is the failure the pseudonymisation was supposed to be free of.
func TestSubscriberLogLabel_IsTheSameTokenTheMetricLabelPublishes(t *testing.T) {
	registryID := "sub_0f1e2d3c4b5a69788796a5b4c3d2e1f0"

	require.True(t, isRegistrySubscriberIdentifier(registryID),
		"the fixture has to be an identifier the registry admits, or the assertion is vacuous")

	assert.Equal(t, subscriberLagLabel(registryID), subscriberLogLabel(registryID),
		"one identifier must produce ONE token in the metric and in the log")
	assert.Equal(t, model.HashIdentifier(registryID), subscriberLogLabel(registryID),
		"and that token must be the canonical rule, which the API response also publishes")

	t.Run("the raw identifier never appears in the token", func(t *testing.T) {
		assert.NotContains(t, subscriberLogLabel(registryID), registryID)
		assert.Len(t, subscriberLogLabel(registryID), model.LogIdentifierHashLength)
	})

	t.Run("an empty id is the unattributed classification rather than a digest", func(t *testing.T) {
		assert.Equal(t, lagLabelUnattributed, subscriberLogLabel(""))
		assert.Equal(t, lagLabelUnattributed, subscriberLogLabel("   "))
	})

	t.Run("an unadmitted id is HASHED in a log and collapsed on a metric", func(t *testing.T) {
		// The one deliberate divergence. A log has no cardinality budget and does have to
		// keep two rogue identifiers apart; a metric has the opposite constraint. Nothing
		// is lost for the pivot, because an id the registry does not admit is not in the
		// registry to be resolved.
		//
		// The fixtures carry an uppercase letter and a space respectively, which is what
		// makes them unadmitted: CanonicalizeSubscriberIdentifier permits lowercase
		// alphanumerics with '_' and '-' only, so an ordinary tenant-ish name such as
		// "acme-payments-eu" IS admissible and would not exercise this path.
		rogue, other := "Acme Payments EU", "Globex Treasury"

		require.False(t, isRegistrySubscriberIdentifier(rogue))
		require.False(t, isRegistrySubscriberIdentifier(other))

		assert.Equal(t, lagLabelUnregistered, subscriberLagLabel(rogue))
		assert.Equal(t, lagLabelUnregistered, subscriberLagLabel(other))
		assert.NotEqual(t, subscriberLogLabel(rogue), subscriberLogLabel(other),
			"two unadmitted identifiers must stay distinguishable in a log")
		assert.NotContains(t, subscriberLogLabel(rogue), "Acme",
			"and neither may be readable")
	})
}

// TestConsumerGroupLogLabel_HashesTheNamespaceRootLikeTheMetricDoes covers the group half.
func TestConsumerGroupLogLabel_HashesTheNamespaceRootLikeTheMetricDoes(t *testing.T) {
	subscriberID := "sub_0f1e2d3c4b5a69788796a5b4c3d2e1f0"

	root, err := model.CanonicalConsumerGroupNamespace(subscriberID)
	require.NoError(t, err, "the fixture must be a group the resolver recognises a root in")

	// A subscriber's runtime group legitimately EXTENDS its namespace: the ACL grants Read on
	// the group as a prefixed pattern, so three consumer instances may commit under three
	// leaves. consumerGroupRoot requires the terminator, so the leaf is built with it.
	group, err := model.CanonicalConsumerGroupID(subscriberID)
	require.NoError(t, err)

	suffixed := root + model.SubscriberGroupTerminator + "worker-1"
	require.True(t, isRegistryConsumerGroupID(suffixed),
		"the suffixed fixture must be a leaf the resolver reduces, or the assertion is vacuous")
	require.True(t, isRegistryConsumerGroupID(group))

	assert.Equal(t, consumerGroupLagLabel(suffixed), consumerGroupLogLabel(suffixed),
		"one group must produce ONE token in the metric and in the log")
	assert.Equal(t, consumerGroupLogLabel(group), consumerGroupLogLabel(suffixed),
		"every group a subscriber runs collapses to its namespace root, so the token is stable")
	assert.NotContains(t, consumerGroupLogLabel(suffixed), subscriberID,
		"the group id is derived from the subscriber id, so publishing it would disclose the "+
			"subscriber by another route")

	assert.Equal(t, lagLabelUnattributed, consumerGroupLogLabel(""))
	assert.NotEqual(t, lagLabelUnregistered, consumerGroupLogLabel("someone-elses-group"),
		"a log hashes a group with no recognisable root rather than collapsing it")
}

// TestHashLogIdentifier_DelegatesToTheOneCanonicalRule guards against a second implementation.
//
// Three packages publish this token — the root package on metrics and logs, the database
// package on its own log lines, and api/model on the subscriber resource — and drift between
// them is silent: two tokens for one subscriber, and a resolver that returns nothing.
func TestHashLogIdentifier_DelegatesToTheOneCanonicalRule(t *testing.T) {
	for _, value := range []string{
		"sub_0f1e2d3c4b5a69788796a5b4c3d2e1f0",
		"ldg_9a8b7c6d",
		"bln_1122334455",
		"a",
	} {
		assert.Equal(t, model.HashIdentifier(value), hashLogIdentifier(value),
			"the root package's hash must BE the canonical rule, not merely agree with it today")
	}

	assert.Empty(t, hashLogIdentifier(""),
		"an empty input stays empty, so 'no identifier' and 'some identifier' remain distinct")
	assert.Len(t, hashLogIdentifier("sub_1"), model.LogIdentifierHashLength)
	assert.Equal(t, model.LogIdentifierHashLength, logIdentifierHashLength,
		"the root package's length constant is the canonical one, not a copy of its value")
}

// TestPublishResultLogFields_KeepsTheErrorReasonBecauseR4MandatesIt is the documented
// exception, asserted rather than assumed.
//
// AAP requirement R-4: "The attempt count and error reason must be logged on every attempt,
// not only on final failure", with §0.1.3 naming the field list. A future sweep that
// classified this field too would satisfy the general disclosure rule and BREAK the AAP, so
// the exception is pinned here where such a sweep would trip over it.
func TestPublishResultLogFields_KeepsTheErrorReasonBecauseR4MandatesIt(t *testing.T) {
	fields := PublishResult{
		Status:      model.PublishStatusRetrying,
		EventID:     "3f1d6b0e-6d3c-4f21-9c8a-6a1f0c1d2e3b",
		EventType:   "transaction.applied",
		Topic:       "blnk.transactions",
		Attempt:     3,
		MaxAttempts: 5,
		Err:         errors.New("connection refused"),
	}.LogFields()

	require.Contains(t, fields, "error",
		"R-4 mandates the error reason on every attempt; it is not subject to the general "+
			"classification rule")
	assert.Contains(t, fmt.Sprint(fields["error"]), "connection refused")
	assert.Equal(t, 3, fields["attempt"], "and the attempt count is the other half of R-4")
}

// TestEventLogFields_CarryNoRawSubscriberOrGroupIdentifier is the lasting structural guard.
//
// Behaviour tests cover the resolvers; this covers the CALL SITES, which is where the defect
// actually lived — the resolvers existed and were correct, and the log lines simply did not
// use them. A new log line added later would reintroduce it silently, and there is no runtime
// assertion that can see a field nobody happened to exercise.
//
// The forbidden set is the field KEYS rather than the values: a key named "subscriber" is the
// one an aggregator indexes and an operator greps, so the rule is that those keys do not exist
// in these files at all.
func TestEventLogFields_CarryNoRawSubscriberOrGroupIdentifier(t *testing.T) {
	forbidden := map[string]string{
		"subscriber":    "use subscriber_id_hash with subscriberLogLabel",
		"subscriber_id": "use subscriber_id_hash with subscriberLogLabel",
		"principal":     "use principal_hash with subscriberLogLabel",
		"group":         "use consumer_group_hash with consumerGroupLogLabel",

		// THE GROUP'S THREE SPELLINGS, all of them. A consumer group id and a consumer group
		// prefix are DERIVED from the subscriber id — CanonicalConsumerGroupID composes one from
		// the other — so a log line publishing either discloses the subscriber by another route,
		// which is exactly what TestConsumerGroupLogLabel_HashesTheNamespaceRootLikeTheMetricDoes
		// asserts of the token. Listing only the bare "consumer_group" left the rule with a hole
		// two live call sites were sitting in, so all three names are named.
		"consumer_group":        "use consumer_group_hash with consumerGroupLogLabel",
		"consumer_group_id":     "use consumer_group_hash with consumerGroupLogLabel",
		"consumer_group_prefix": "use consumer_group_hash with consumerGroupLogLabel",
	}

	for _, name := range []string{"event_admin.go", "event_subscriber.go", "event_metrics.go"} {
		t.Run(name, func(t *testing.T) {
			file := parseRepositoryGoFile(t, name)

			for _, key := range logFieldKeys(file) {
				remedy, banned := forbidden[key]
				assert.False(t, banned,
					"%s builds a log field keyed %q, which carries a tenant-derived identifier "+
						"in plaintext into an indexed log; %s", name, key, remedy)
			}
		})
	}
}

// logFieldKeys collects the string keys of every logrus.Fields composite literal in the file,
// plus the first argument of every WithField call.
//
// Keys only. The VALUE is not inspected, deliberately: a value is an arbitrary expression and
// judging it would need the very analysis this avoids, while the key is a literal and is the
// thing a log aggregator indexes. A field whose key is bounded and whose value is a hash is
// the shape being enforced, and the key is sufficient to enforce it because the pseudonymised
// keys are named differently from the raw ones.
//
// Parameters:
//   - file *ast.File: the parsed file.
//
// Returns:
//   - []string: the keys, unquoted, with duplicates retained so a count is possible.
func logFieldKeys(file *ast.File) []string {
	var keys []string

	unquote := func(expr ast.Expr) (string, bool) {
		literal, ok := expr.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return "", false
		}

		return strings.Trim(literal.Value, `"`), true
	}

	ast.Inspect(file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.CompositeLit:
			// logrus.Fields{...} — a map literal whose keys are the field names.
			selector, ok := typed.Type.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Fields" {
				return true
			}
			for _, element := range typed.Elts {
				pair, isPair := element.(*ast.KeyValueExpr)
				if !isPair {
					continue
				}
				if key, isString := unquote(pair.Key); isString {
					keys = append(keys, key)
				}
			}
		case *ast.CallExpr:
			// WithField("name", value) — the first argument is the field name.
			selector, ok := typed.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "WithField" || len(typed.Args) == 0 {
				return true
			}
			if key, isString := unquote(typed.Args[0]); isString {
				keys = append(keys, key)
			}
		}

		return true
	})

	return keys
}

// pinLogLevel sets the standard logger's level for one test and returns the restorer.
//
// Restoring matters more than it looks: the level is process-wide, and a test that left it at
// trace would make every later test in the package emit its diagnostics, changing what other
// hook-based assertions observe.
//
// Parameters:
//   - t *testing.T: the test, for the helper marker.
//   - level logrus.Level: the level to pin.
//
// Returns:
//   - func(): restores the previous level. Call it, conventionally by defer.
func pinLogLevel(t *testing.T, level logrus.Level) func() {
	t.Helper()

	previous := logrus.GetLevel()
	logrus.SetLevel(level)

	return func() { logrus.SetLevel(previous) }
}

// pagingSubscriberStore is a registry double that honours limit and offset.
//
// The package's general subscriberTestStore deliberately ignores both and returns its rows in
// map order, which is right for the operations that do not page and useless here: the whole
// subject of these tests is what happens BEYOND the first page, and a double that returned
// everything at once would make every one of them pass without exercising the walk. Only
// ListEventSubscribers is overridden; everything else is inherited.
type pagingSubscriberStore struct {
	*subscriberTestStore

	// ordered is the registry in a stable order, which is what makes a page boundary a
	// meaningful position rather than an accident of map iteration.
	ordered []model.EventSubscriber

	// listCalls counts pages read, so a test can assert that the walk STOPPED rather than
	// inferring it from the answer.
	listCalls int

	// failAfter, when positive, fails the walk on that page. Used to prove a mid-walk
	// repository failure is reported rather than answered as "not found".
	failAfter int
}

// newPagingSubscriberStore builds a registry of size rows with canonical identifiers.
func newPagingSubscriberStore(t *testing.T, size int) *pagingSubscriberStore {
	t.Helper()

	store := &pagingSubscriberStore{subscriberTestStore: newSubscriberTestStore(newSubscriberCallLog())}
	registeredAt := time.Now().UTC()

	for i := 0; i < size; i++ {
		// Canonical identifiers, because subscriberLogLabel only pseudonymises what the
		// registry admits and a rejected fixture would make the assertions vacuous.
		identifier := fmt.Sprintf("sub_%032x", i)
		// A DISTINCT (created_at, id) per row, because that pair IS the keyset cursor. The real
		// table cannot produce a duplicate — id is a BIGSERIAL and created_at is NOT NULL — and a
		// fixture that left both zero would make every cursor resolve to the first row, so a walk
		// would read the same page for ever and report the page ceiling on a registry of three.
		store.ordered = append(store.ordered, model.EventSubscriber{
			ID:           int64(size - i),
			SubscriberID: identifier,
			CreatedAt:    registeredAt.Add(-time.Duration(i) * time.Second),
		})
		require.True(t, isRegistrySubscriberIdentifier(identifier))
	}

	return store
}

// ListEventSubscribers returns one page, honouring limit and offset.
func (s *pagingSubscriberStore) ListEventSubscribers(
	_ context.Context,
	query model.SubscriberPageQuery,
) (model.SubscriberPage, error) {
	s.listCalls++

	if s.failAfter > 0 && s.listCalls >= s.failAfter {
		return model.SubscriberPage{}, errors.New(
			"pq: connection reset by peer while listing blnk.event_subscribers")
	}

	// Resumed from the cursor exactly as the repository does: the row it names belongs to the
	// previous page, so the walk continues from the one after it.
	start := 0
	if query.Cursor != nil {
		start = len(s.ordered)

		for i := range s.ordered {
			if s.ordered[i].CreatedAt.Equal(query.Cursor.CreatedAt) && s.ordered[i].ID == query.Cursor.ID {
				start = i + 1

				break
			}
		}
	}

	if start >= len(s.ordered) {
		return model.SubscriberPage{}, nil
	}

	limit := query.Limit
	if limit <= 0 {
		limit = len(s.ordered)
	}

	end := start + limit
	if end > len(s.ordered) {
		end = len(s.ordered)
	}

	page := model.SubscriberPage{
		Subscribers: make([]model.EventSubscriber, end-start),
		HasMore:     end < len(s.ordered),
	}
	copy(page.Subscribers, s.ordered[start:end])

	if page.HasMore {
		last := page.Subscribers[len(page.Subscribers)-1]
		page.NextCursor = &model.SubscriberCursor{CreatedAt: last.CreatedAt, ID: last.ID}
	}

	return page, nil
}

// TestResolveSubscriberByPseudonym_WalksEveryPageRatherThanTheFirst is F16's resolver.
//
// The documented procedure before this existed was to fetch one page and hash the rows
// locally, which answers correctly only while the registry fits in that page and fails
// SILENTLY past it — reporting "no match" for subscribers it never read, so an operator
// concludes the alert names a subscriber that no longer exists and closes a live incident.
func TestResolveSubscriberByPseudonym_WalksEveryPageRatherThanTheFirst(t *testing.T) {
	t.Run("a subscriber past the first page is found", func(t *testing.T) {
		store := newPagingSubscriberStore(t, 250)
		service := NewEventSubscriberService(store, nil)

		// Deliberately on the third page. One page of 100 is exactly the reach of the old
		// procedure, so a fixture inside it would pass either way.
		wanted := store.ordered[212]

		resolved, err := service.ResolveSubscriberByPseudonym(
			context.Background(), model.HashIdentifier(wanted.SubscriberID),
		)

		require.NoError(t, err)
		require.NotNil(t, resolved)
		assert.Equal(t, wanted.SubscriberID, resolved.SubscriberID)
		assert.Equal(t, 3, store.listCalls, "three pages were needed and three were read")
	})

	t.Run("the walk stops at the match rather than reading the whole registry", func(t *testing.T) {
		store := newPagingSubscriberStore(t, 500)
		service := NewEventSubscriberService(store, nil)

		_, err := service.ResolveSubscriberByPseudonym(
			context.Background(), model.HashIdentifier(store.ordered[4].SubscriberID),
		)

		require.NoError(t, err)
		assert.Equal(t, 1, store.listCalls, "a first-page match must not page on")
	})

	t.Run("an exhausted registry answers NOT FOUND, and only then", func(t *testing.T) {
		store := newPagingSubscriberStore(t, 150)
		service := NewEventSubscriberService(store, nil)

		_, err := service.ResolveSubscriberByPseudonym(context.Background(), "0123456789abcdef")

		require.Error(t, err)
		var typed apierror.APIError
		require.ErrorAs(t, err, &typed)
		assert.Equal(t, apierror.ErrSubscriberNotFound, typed.Code,
			"a short page is the end of the registry, so the token is genuinely unknown")
		assert.Equal(t, 2, store.listCalls,
			"the second page was short, which is what proves the registry was exhausted")
	})

	t.Run("a token is matched case-insensitively", func(t *testing.T) {
		store := newPagingSubscriberStore(t, 5)
		service := NewEventSubscriberService(store, nil)

		token := model.HashIdentifier(store.ordered[2].SubscriberID)

		resolved, err := service.ResolveSubscriberByPseudonym(
			context.Background(), "  "+strings.ToUpper(token)+"  ",
		)

		require.NoError(t, err, "an operator copying a token out of a dashboard must not be "+
			"defeated by its casing or by surrounding whitespace")
		assert.Equal(t, store.ordered[2].SubscriberID, resolved.SubscriberID)
	})

	t.Run("a blank token is refused rather than walked", func(t *testing.T) {
		store := newPagingSubscriberStore(t, 5)
		service := NewEventSubscriberService(store, nil)

		_, err := service.ResolveSubscriberByPseudonym(context.Background(), "   ")

		require.Error(t, err)
		var typed apierror.APIError
		require.ErrorAs(t, err, &typed)
		assert.Equal(t, apierror.ErrInvalidInput, typed.Code)
		assert.Zero(t, store.listCalls, "nothing may be read for a token that cannot match")
	})

	t.Run("a mid-walk repository failure is reported, never answered as not found", func(t *testing.T) {
		store := newPagingSubscriberStore(t, 500)
		store.failAfter = 2
		service := NewEventSubscriberService(store, nil)

		_, err := service.ResolveSubscriberByPseudonym(context.Background(), "0123456789abcdef")

		require.Error(t, err)
		var typed apierror.APIError
		if errors.As(err, &typed) {
			assert.NotEqual(t, apierror.ErrSubscriberNotFound, typed.Code,
				"a failed read establishes nothing about whether the token exists")
		}
		assert.NotErrorIs(t, err, ErrSubscriberHashResolutionExhausted)
	})

	t.Run("the page ceiling is reported as UNKNOWN, not as absent", func(t *testing.T) {
		// A registry larger than the bound. The distinction is the point: answering "not
		// found" here would state something the resolver did not establish, and that
		// statement is the one that closes a live incident.
		store := newPagingSubscriberStore(t, 0)
		store.ordered = nil
		for i := 0; i < subscriberHashResolutionMaxPages*subscriberHashResolutionPageSize+1; i++ {
			store.ordered = append(store.ordered,
				model.EventSubscriber{SubscriberID: fmt.Sprintf("sub_%032x", i)})
		}
		service := NewEventSubscriberService(store, nil)

		_, err := service.ResolveSubscriberByPseudonym(context.Background(), "0123456789abcdef")

		require.ErrorIs(t, err, ErrSubscriberHashResolutionExhausted)
		assert.Equal(t, subscriberHashResolutionMaxPages, store.listCalls,
			"the ceiling bounds the work, so exactly that many pages are read and no more")
	})

	t.Run("a cancelled caller stops the walk", func(t *testing.T) {
		store := newPagingSubscriberStore(t, 500)
		service := NewEventSubscriberService(store, nil)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := service.ResolveSubscriberByPseudonym(ctx, "0123456789abcdef")

		require.ErrorIs(t, err, context.Canceled)
		assert.Zero(t, store.listCalls, "cancellation is checked before the first read")
	})

	t.Run("a service with no store reports that rather than panicking", func(t *testing.T) {
		_, err := NewEventSubscriberService(nil, nil).
			ResolveSubscriberByPseudonym(context.Background(), "0123456789abcdef")

		require.Error(t, err)
		// The code, not the wrapped sentinel: apierror.APIError carries its cause in an
		// interface{} Details field and exposes no Unwrap, so errors.Is cannot reach it. That
		// is the same reason the repository stopped putting driver errors there at all.
		var typed apierror.APIError
		require.ErrorAs(t, err, &typed)
		assert.Equal(t, apierror.ErrInternalServer, typed.Code)
	})
}

// TestListSubscribersAwaitingRevocation_IsTheAlertsFirstStep covers F24.
//
// SubscriberRevocationOutstanding is CRITICAL and carries no subscriber attribute, because a
// label would export a tenant identifier into every notification. Its first remediation step is
// therefore "find which subscribers are affected", and before this existed the alert told a
// responder to read `revocation_pending` from GET /subscribers — a field the API did not have.
// A critical alert whose first step is impossible spends the exposure window on a search.
func TestListSubscribersAwaitingRevocation_IsTheAlertsFirstStep(t *testing.T) {
	pendingAt := func(offset time.Duration) *time.Time {
		instant := time.Now().UTC().Add(-offset)

		return &instant
	}

	t.Run("only the marked rows are returned, oldest exposure first", func(t *testing.T) {
		store := newPagingSubscriberStore(t, 250)
		// Deliberately spread across page boundaries and seeded OUT of order, so the ordering
		// assertion cannot pass by accident of the registry's own order.
		store.ordered[7].RevocationPendingAt = pendingAt(30 * time.Minute)
		store.ordered[120].RevocationPendingAt = pendingAt(4 * time.Hour)
		store.ordered[240].RevocationPendingAt = pendingAt(90 * time.Minute)

		service := NewEventSubscriberService(store, nil)

		pending, err := service.ListSubscribersAwaitingRevocation(context.Background())

		require.NoError(t, err)
		require.Len(t, pending, 3, "a marked row on the third page must not be missed")
		assert.Equal(t, store.ordered[120].SubscriberID, pending[0].SubscriberID,
			"oldest obligation first: the alert fires on the oldest age, so that row belongs at "+
				"the top rather than leaving the responder to compare timestamps")
		assert.Equal(t, store.ordered[240].SubscriberID, pending[1].SubscriberID)
		assert.Equal(t, store.ordered[7].SubscriberID, pending[2].SubscriberID)
	})

	t.Run("the ordinary state is an empty, non-nil result", func(t *testing.T) {
		store := newPagingSubscriberStore(t, 40)
		service := NewEventSubscriberService(store, nil)

		pending, err := service.ListSubscribersAwaitingRevocation(context.Background())

		require.NoError(t, err)
		require.NotNil(t, pending, "a non-nil slice serialises as [] rather than null")
		assert.Empty(t, pending)
	})

	t.Run("an incomplete scan FAILS rather than returning a shorter list", func(t *testing.T) {
		// The rows are live SASL credentials nobody is accounted for. A truncated list reads
		// exactly like a complete one, so a responder would revoke those, close the incident,
		// and leave the rest authenticating.
		store := newPagingSubscriberStore(t, 0)
		store.ordered = nil
		for i := 0; i < subscriberHashResolutionMaxPages*subscriberHashResolutionPageSize+1; i++ {
			store.ordered = append(store.ordered,
				model.EventSubscriber{SubscriberID: fmt.Sprintf("sub_%032x", i)})
		}
		store.ordered[0].RevocationPendingAt = pendingAt(2 * time.Hour)

		service := NewEventSubscriberService(store, nil)

		pending, err := service.ListSubscribersAwaitingRevocation(context.Background())

		require.ErrorIs(t, err, ErrSubscriberHashResolutionExhausted)
		assert.Nil(t, pending, "no partial list may be handed back with the error")
	})

	t.Run("a repository failure is reported", func(t *testing.T) {
		store := newPagingSubscriberStore(t, 500)
		store.failAfter = 1
		service := NewEventSubscriberService(store, nil)

		_, err := service.ListSubscribersAwaitingRevocation(context.Background())

		require.Error(t, err)
		assert.NotErrorIs(t, err, ErrSubscriberHashResolutionExhausted)
	})

	t.Run("a cancelled caller stops the scan", func(t *testing.T) {
		store := newPagingSubscriberStore(t, 500)
		service := NewEventSubscriberService(store, nil)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := service.ListSubscribersAwaitingRevocation(ctx)

		require.ErrorIs(t, err, context.Canceled)
		assert.Zero(t, store.listCalls)
	})
}

// TestSubscriberResponse_PublishesTheRevocationMarkerTheAlertTellsRespondersentToRead pins the
// wire shape F24 depends on, at the JSON level rather than the struct level.
//
// The struct field existing is not the contract; the KEY appearing in the body is, and the
// boolean must appear even when false — an omitted false makes "settled" and "this version does
// not report it" the same wire state.
func TestSubscriberResponse_PublishesTheRevocationMarkerTheAlertTellsRespondersToRead(t *testing.T) {
	settled := apimodel.NewSubscriberResponse(model.EventSubscriber{
		SubscriberID: "sub_0f1e2d3c4b5a69788796a5b4c3d2e1f0",
		Name:         "ledger-ops",
	})

	body, err := json.Marshal(settled)
	require.NoError(t, err)

	var decoded map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &decoded))

	require.Contains(t, decoded, "revocation_pending",
		"the boolean must be present on every subscriber, settled or not")
	assert.JSONEq(t, "false", string(decoded["revocation_pending"]))
	assert.NotContains(t, decoded, "revocation_pending_at",
		"the instant is omitted when there is no obligation, which is what makes its presence "+
			"meaningful")

	pendingSince := time.Date(2026, 4, 1, 9, 30, 0, 0, time.UTC)
	outstanding := apimodel.NewSubscriberResponse(model.EventSubscriber{
		SubscriberID:        "sub_0f1e2d3c4b5a69788796a5b4c3d2e1f0",
		Name:                "ledger-ops",
		KafkaPrincipal:      "blnk-sub-sub_0f1e2d3c4b5a69788796a5b4c3d2e1f0",
		RevocationPendingAt: &pendingSince,
	})

	assert.True(t, outstanding.RevocationPending)
	require.NotNil(t, outstanding.RevocationPendingAt)
	assert.Equal(t, pendingSince, *outstanding.RevocationPendingAt)
	assert.NotEmpty(t, outstanding.KafkaPrincipal,
		"the responder revokes by principal, so the row has to carry it beside the marker")
}
