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
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/embedded"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

// Every test in this file runs WITHOUT A BROKER, against a fake that records the requests
// it is handed. That is not a convenience: the properties this file has to pin have no
// runtime failure mode on a real broker either. A topic created with one partition
// instead of six works. A wildcard ACL pattern is accepted. A replication factor of 3 on
// a one-broker cluster fails, but a hard-coded 1 in production does not. A negative lag
// is published happily by any gauge. The only way to catch each of those is to assert the
// request that goes out and the arithmetic that produced it, which is exactly what a fake
// makes possible and a live broker makes hard.
//
// Broker-backed proof belongs elsewhere by design: that ACLs are ENFORCED can only be
// shown against a broker running the KRaft StandardAuthorizer, and that lives in the
// isolation integration test. Keeping this file broker-free is what keeps
// `go test -short ./...` and CI green with no Kafka service.
//
// The eight topic names, the six-partition floor, the 4096-iteration minimum and the
// three gauge attribute keys are all written out LONGHAND below rather than derived from
// the code under test. A test that asks the implementation what it expects agrees with
// any implementation, including a broken one.

// expectedEventTopics is the topic inventory, spelled out independently of
// event_topics.go: the four category topics followed by their four dead-letter siblings,
// in the canonical order provisioning uses.
var expectedEventTopics = []string{
	"blnk.transactions",
	"blnk.balances",
	"blnk.identities",
	"blnk.system",
	"blnk.transactions.dlt",
	"blnk.balances.dlt",
	"blnk.identities.dlt",
	"blnk.system.dlt",
}

// fakeAdminClient is a stateful stand-in for kafka-go's Client.
//
// It models the broker behaviour the implementation has to cope with — a topic that
// already exists, a partition count that cannot be reduced, a consumer group that has
// never committed, a partition whose offsets cannot be read, a broker with no authorizer
// — and records every request so the outgoing arguments can be asserted.
//
// It is mutex-guarded because the admin client is shared and the whole point of sharing
// it is concurrent use; the guard is what lets `go test -race` exercise that.
type fakeAdminClient struct {
	mu sync.Mutex

	// --- Modelled broker state ---

	// partitions maps an existing topic to its partition IDs.
	partitions map[string][]int
	// first and end are the offset bounds per topic and partition.
	first map[string]map[int]int64
	end   map[string]map[int]int64
	// committed is the consumer group's committed offsets per topic and partition.
	committed map[string]map[int]int64
	// scram maps a principal to the SCRAM mechanisms it holds credentials for.
	scram map[string][]kafka.ScramMechanism
	// securityDisabled models a broker started without an authorizer: it answers
	// SECURITY_DISABLED to DescribeACLs while still accepting CreateACLs.
	securityDisabled bool
	// groupNotFound models an OffsetFetch for a consumer group that does not exist.
	groupNotFound bool
	// racedPartitions is the partition count a topic turns out to have when the fake
	// answers TOPIC_ALREADY_EXISTS, modelling a concurrent provisioner's creation.
	racedPartitions int

	// --- Injected failures ---

	// transportErrors fails a whole call by method name, modelling an unreachable
	// broker.
	transportErrors map[string]error
	// createTopicErrors overrides the per-topic result of CreateTopics.
	createTopicErrors map[string]error
	// metadataTopicErrors overrides the per-topic error in a metadata response.
	metadataTopicErrors map[string]error
	// offsetErrors fails individual partitions in a ListOffsets response.
	offsetErrors map[string]map[int]error
	// committedErrors fails individual partitions in an OffsetFetch response.
	committedErrors map[string]map[int]error
	// scramUpsertError is returned as the per-user result of an upsert.
	scramUpsertError error
	// scramUpsertNoResult makes the upsert answer about nobody at all.
	scramUpsertNoResult bool
	// aclErrors is returned positionally by CreateACLs.
	aclErrors []error

	// --- Recordings ---

	calls                    []string
	deadlines                []bool
	createTopicsRequests     []*kafka.CreateTopicsRequest
	createPartitionsRequests []*kafka.CreatePartitionsRequest
	createACLsRequests       []*kafka.CreateACLsRequest
	describeACLsRequests     []*kafka.DescribeACLsRequest
	scramUpsertRequests      []*kafka.AlterUserScramCredentialsRequest
	describeScramRequests    []*kafka.DescribeUserScramCredentialsRequest
	metadataRequests         []*kafka.MetadataRequest
	listOffsetsRequests      []*kafka.ListOffsetsRequest
	offsetFetchRequests      []*kafka.OffsetFetchRequest
}

// newFakeAdminClient returns a fake modelling an empty, healthy, ACL-enforcing broker.
func newFakeAdminClient() *fakeAdminClient {
	return &fakeAdminClient{
		partitions:          map[string][]int{},
		first:               map[string]map[int]int64{},
		end:                 map[string]map[int]int64{},
		committed:           map[string]map[int]int64{},
		scram:               map[string][]kafka.ScramMechanism{},
		racedPartitions:     MinTopicPartitions,
		transportErrors:     map[string]error{},
		createTopicErrors:   map[string]error{},
		metadataTopicErrors: map[string]error{},
		offsetErrors:        map[string]map[int]error{},
		committedErrors:     map[string]map[int]error{},
	}
}

// withTopic adds an existing topic with the given number of partitions, numbered from
// zero, and gives every partition an empty log.
func (f *fakeAdminClient) withTopic(topic string, partitions int) *fakeAdminClient {
	ids := make([]int, 0, partitions)
	for id := 0; id < partitions; id++ {
		ids = append(ids, id)
	}
	f.partitions[topic] = ids

	return f
}

// withOffsets sets one partition's offset window, creating the topic if needed.
func (f *fakeAdminClient) withOffsets(topic string, partition int, first, end int64) *fakeAdminClient {
	if _, exists := f.partitions[topic]; !exists {
		f.partitions[topic] = []int{}
	}
	if !containsPartition(f.partitions[topic], partition) {
		f.partitions[topic] = append(f.partitions[topic], partition)
	}

	if f.first[topic] == nil {
		f.first[topic] = map[int]int64{}
	}
	if f.end[topic] == nil {
		f.end[topic] = map[int]int64{}
	}
	f.first[topic][partition] = first
	f.end[topic][partition] = end

	return f
}

// withCommitted sets one partition's committed offset for the consumer group.
func (f *fakeAdminClient) withCommitted(topic string, partition int, offset int64) *fakeAdminClient {
	if f.committed[topic] == nil {
		f.committed[topic] = map[int]int64{}
	}
	f.committed[topic][partition] = offset

	return f
}

// containsPartition reports whether ids already holds partition.
func containsPartition(ids []int, partition int) bool {
	for _, id := range ids {
		if id == partition {
			return true
		}
	}

	return false
}

// record notes a call, whether its context carried a deadline, and returns any injected
// transport failure for that method.
func (f *fakeAdminClient) record(method string, ctx context.Context) error {
	f.calls = append(f.calls, method)

	_, hasDeadline := ctx.Deadline()
	f.deadlines = append(f.deadlines, hasDeadline)

	return f.transportErrors[method]
}

// callCount returns how many times a method was called.
func (f *fakeAdminClient) callCount(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()

	count := 0
	for _, call := range f.calls {
		if call == method {
			count++
		}
	}

	return count
}

// totalCalls returns how many requests reached the fake in total.
func (f *fakeAdminClient) totalCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.calls)
}

// everyCallHadADeadline reports whether every recorded call carried a context deadline.
func (f *fakeAdminClient) everyCallHadADeadline() bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, hasDeadline := range f.deadlines {
		if !hasDeadline {
			return false
		}
	}

	return len(f.deadlines) > 0
}

func (f *fakeAdminClient) Metadata(
	ctx context.Context,
	req *kafka.MetadataRequest,
) (*kafka.MetadataResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.record("Metadata", ctx); err != nil {
		return nil, err
	}
	f.metadataRequests = append(f.metadataRequests, req)

	response := &kafka.MetadataResponse{ClusterID: "fake-cluster"}
	for _, name := range req.Topics {
		topic := kafka.Topic{Name: name}

		if injected, ok := f.metadataTopicErrors[name]; ok {
			topic.Error = injected
			response.Topics = append(response.Topics, topic)

			continue
		}

		ids, exists := f.partitions[name]
		if !exists {
			topic.Error = kafka.UnknownTopicOrPartition
			response.Topics = append(response.Topics, topic)

			continue
		}

		for _, id := range ids {
			topic.Partitions = append(topic.Partitions, kafka.Partition{Topic: name, ID: id})
		}
		response.Topics = append(response.Topics, topic)
	}

	return response, nil
}

func (f *fakeAdminClient) CreateTopics(
	ctx context.Context,
	req *kafka.CreateTopicsRequest,
) (*kafka.CreateTopicsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.record("CreateTopics", ctx); err != nil {
		return nil, err
	}
	f.createTopicsRequests = append(f.createTopicsRequests, req)

	response := &kafka.CreateTopicsResponse{Errors: map[string]error{}}
	for _, cfg := range req.Topics {
		if injected, ok := f.createTopicErrors[cfg.Topic]; ok {
			response.Errors[cfg.Topic] = injected
			if errors.Is(injected, kafka.TopicAlreadyExists) {
				// Model the concurrent provisioner that won the race: the topic really
				// does exist by the time the caller re-probes.
				f.withTopic(cfg.Topic, f.racedPartitions)
			}

			continue
		}

		if _, exists := f.partitions[cfg.Topic]; exists {
			response.Errors[cfg.Topic] = kafka.TopicAlreadyExists

			continue
		}

		f.withTopic(cfg.Topic, cfg.NumPartitions)
		response.Errors[cfg.Topic] = nil
	}

	return response, nil
}

func (f *fakeAdminClient) CreatePartitions(
	ctx context.Context,
	req *kafka.CreatePartitionsRequest,
) (*kafka.CreatePartitionsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.record("CreatePartitions", ctx); err != nil {
		return nil, err
	}
	f.createPartitionsRequests = append(f.createPartitionsRequests, req)

	response := &kafka.CreatePartitionsResponse{Errors: map[string]error{}}
	for _, cfg := range req.Topics {
		ids, exists := f.partitions[cfg.Name]
		if !exists {
			response.Errors[cfg.Name] = kafka.UnknownTopicOrPartition

			continue
		}

		if int32(len(ids)) >= cfg.Count {
			// Kafka's answer when the requested total is not greater than the current
			// count, which is also what a concurrent grow produces.
			response.Errors[cfg.Name] = kafka.InvalidPartitionNumber

			continue
		}

		f.withTopic(cfg.Name, int(cfg.Count))
		response.Errors[cfg.Name] = nil
	}

	return response, nil
}

func (f *fakeAdminClient) CreateACLs(
	ctx context.Context,
	req *kafka.CreateACLsRequest,
) (*kafka.CreateACLsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.record("CreateACLs", ctx); err != nil {
		return nil, err
	}
	f.createACLsRequests = append(f.createACLsRequests, req)

	if f.aclErrors != nil {
		return &kafka.CreateACLsResponse{Errors: f.aclErrors}, nil
	}

	return &kafka.CreateACLsResponse{Errors: make([]error, len(req.ACLs))}, nil
}

func (f *fakeAdminClient) DescribeACLs(
	ctx context.Context,
	req *kafka.DescribeACLsRequest,
) (*kafka.DescribeACLsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.record("DescribeACLs", ctx); err != nil {
		return nil, err
	}
	f.describeACLsRequests = append(f.describeACLsRequests, req)

	if f.securityDisabled {
		return &kafka.DescribeACLsResponse{Error: kafka.SecurityDisabled}, nil
	}

	return &kafka.DescribeACLsResponse{}, nil
}

func (f *fakeAdminClient) AlterUserScramCredentials(
	ctx context.Context,
	req *kafka.AlterUserScramCredentialsRequest,
) (*kafka.AlterUserScramCredentialsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.record("AlterUserScramCredentials", ctx); err != nil {
		return nil, err
	}
	f.scramUpsertRequests = append(f.scramUpsertRequests, req)

	if f.scramUpsertNoResult {
		return &kafka.AlterUserScramCredentialsResponse{}, nil
	}

	response := &kafka.AlterUserScramCredentialsResponse{}
	for _, upsertion := range req.Upsertions {
		response.Results = append(response.Results, kafka.AlterUserScramCredentialsResponseUser{
			User:  upsertion.Name,
			Error: f.scramUpsertError,
		})

		if f.scramUpsertError == nil {
			f.scram[upsertion.Name] = []kafka.ScramMechanism{upsertion.Mechanism}
		}
	}

	return response, nil
}

func (f *fakeAdminClient) DescribeUserScramCredentials(
	ctx context.Context,
	req *kafka.DescribeUserScramCredentialsRequest,
) (*kafka.DescribeUserScramCredentialsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.record("DescribeUserScramCredentials", ctx); err != nil {
		return nil, err
	}
	f.describeScramRequests = append(f.describeScramRequests, req)

	response := &kafka.DescribeUserScramCredentialsResponse{}
	for _, user := range req.Users {
		mechanisms, exists := f.scram[user.Name]
		if !exists {
			response.Results = append(response.Results, kafka.DescribeUserScramCredentialsResponseResult{
				User:  user.Name,
				Error: kafka.ResourceNotFound,
			})

			continue
		}

		result := kafka.DescribeUserScramCredentialsResponseResult{User: user.Name}
		for _, mechanism := range mechanisms {
			result.CredentialInfos = append(result.CredentialInfos,
				kafka.DescribeUserScramCredentialsCredentialInfo{
					Mechanism:  mechanism,
					Iterations: DefaultScramIterations,
				})
		}
		response.Results = append(response.Results, result)
	}

	return response, nil
}

func (f *fakeAdminClient) ListOffsets(
	ctx context.Context,
	req *kafka.ListOffsetsRequest,
) (*kafka.ListOffsetsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.record("ListOffsets", ctx); err != nil {
		return nil, err
	}
	f.listOffsetsRequests = append(f.listOffsetsRequests, req)

	response := &kafka.ListOffsetsResponse{Topics: map[string][]kafka.PartitionOffsets{}}
	for topic, requests := range req.Topics {
		// The real client collapses the first-offset and last-offset requests for one
		// partition into a single PartitionOffsets entry, so the fake must too.
		seen := map[int]struct{}{}
		for _, request := range requests {
			if _, duplicate := seen[request.Partition]; duplicate {
				continue
			}
			seen[request.Partition] = struct{}{}

			offsets := kafka.PartitionOffsets{
				Partition:   request.Partition,
				FirstOffset: -1,
				LastOffset:  -1,
			}
			if injected, ok := f.offsetErrors[topic][request.Partition]; ok {
				offsets.Error = injected
			} else {
				if first, ok := f.first[topic][request.Partition]; ok {
					offsets.FirstOffset = first
				} else {
					offsets.FirstOffset = 0
				}
				if end, ok := f.end[topic][request.Partition]; ok {
					offsets.LastOffset = end
				} else {
					offsets.LastOffset = 0
				}
			}

			response.Topics[topic] = append(response.Topics[topic], offsets)
		}
	}

	return response, nil
}

func (f *fakeAdminClient) OffsetFetch(
	ctx context.Context,
	req *kafka.OffsetFetchRequest,
) (*kafka.OffsetFetchResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.record("OffsetFetch", ctx); err != nil {
		return nil, err
	}
	f.offsetFetchRequests = append(f.offsetFetchRequests, req)

	if f.groupNotFound {
		return &kafka.OffsetFetchResponse{Error: kafka.GroupIdNotFound}, nil
	}

	response := &kafka.OffsetFetchResponse{Topics: map[string][]kafka.OffsetFetchPartition{}}
	for topic, partitions := range req.Topics {
		for _, partition := range partitions {
			entry := kafka.OffsetFetchPartition{Partition: partition, CommittedOffset: -1}

			if injected, ok := f.committedErrors[topic][partition]; ok {
				entry.Error = injected
			} else if offset, ok := f.committed[topic][partition]; ok {
				entry.CommittedOffset = offset
			}

			response.Topics[topic] = append(response.Topics[topic], entry)
		}
	}

	return response, nil
}

// Compile-time proof that the fake is a faithful stand-in for the real client.
var _ kafkaAdminAPI = (*fakeAdminClient)(nil)

// newTestKafkaAdmin builds an admin client around the fake with an explicit geometry.
//
// The geometry is passed in rather than read from configuration so that a test can pin a
// replication factor of 1 and prove it reaches the request unchanged — the assertion that
// catches a hard-coded 3.
func newTestKafkaAdmin(fake *fakeAdminClient, partitions, replicationFactor int) *KafkaAdminClient {
	return &KafkaAdminClient{
		client:            fake,
		brokers:           []string{"broker-1:9092"},
		partitions:        partitions,
		replicationFactor: replicationFactor,
	}
}

// recordedGauge is a stand-in for the shared consumer-lag gauge that keeps what was
// recorded, so the value AND the attribute keys can be asserted.
//
// embedded.Int64Gauge is embedded, not implemented: that is how the OpenTelemetry API
// intends third-party implementations of an instrument interface to be written.
type recordedGauge struct {
	embedded.Int64Gauge

	mu      sync.Mutex
	records []gaugeRecord
}

// gaugeRecord is one recorded measurement.
type gaugeRecord struct {
	value      int64
	attributes map[string]string
}

func (g *recordedGauge) Record(ctx context.Context, value int64, options ...otelmetric.RecordOption) {
	g.mu.Lock()
	defer g.mu.Unlock()

	attributes := map[string]string{}
	recorded := otelmetric.NewRecordConfig(options).Attributes()
	for _, keyValue := range recorded.ToSlice() {
		attributes[string(keyValue.Key)] = keyValue.Value.Emit()
	}

	g.records = append(g.records, gaugeRecord{value: value, attributes: attributes})
}

// Enabled reports that this recorder always processes measurements, which is what makes
// the assertions below deterministic.
func (g *recordedGauge) Enabled(context.Context) bool {
	return true
}

// snapshot returns a copy of the recorded measurements.
func (g *recordedGauge) snapshot() []gaugeRecord {
	g.mu.Lock()
	defer g.mu.Unlock()

	records := make([]gaugeRecord, len(g.records))
	copy(records, g.records)

	return records
}

// captureConsumerLagGauge swaps the shared gauge for a recorder for the duration of one
// test.
//
// Swapping the package-level instrument variable keeps the test entirely local: no global
// meter provider is installed, so no other test in the binary is affected, and the real
// instrument is restored on cleanup.
func captureConsumerLagGauge(t *testing.T) *recordedGauge {
	t.Helper()

	recorder := &recordedGauge{}
	original := metrics.SubscriberConsumerLag
	t.Cleanup(func() { metrics.SubscriberConsumerLag = original })
	metrics.SubscriberConsumerLag = recorder

	return recorder
}

// parseEventAdminSource parses event_admin.go into an AST, comments included, so
// structural guarantees about the implementation can be asserted rather than trusted.
func parseEventAdminSource(t *testing.T) (*ast.File, *token.FileSet) {
	t.Helper()

	fileSet := token.NewFileSet()
	path := filepath.Join(moduleRootDir(t), "event_admin.go")
	parsed, err := parser.ParseFile(fileSet, path, nil, parser.ParseComments)
	require.NoError(t, err, "event_admin.go must be parseable to assert its structure")

	return parsed, fileSet
}

// createdTopicConfigs flattens every topic configuration the fake was asked to create.
func createdTopicConfigs(fake *fakeAdminClient) []kafka.TopicConfig {
	fake.mu.Lock()
	defer fake.mu.Unlock()

	configs := make([]kafka.TopicConfig, 0, len(expectedEventTopics))
	for _, request := range fake.createTopicsRequests {
		configs = append(configs, request.Topics...)
	}

	return configs
}

// grownTopicConfigs flattens every partition growth the fake was asked to perform.
func grownTopicConfigs(fake *fakeAdminClient) []kafka.TopicPartitionsConfig {
	fake.mu.Lock()
	defer fake.mu.Unlock()

	configs := make([]kafka.TopicPartitionsConfig, 0, len(expectedEventTopics))
	for _, request := range fake.createPartitionsRequests {
		configs = append(configs, request.Topics...)
	}

	return configs
}

// requestedACLs flattens every ACL binding the fake was asked to create.
func requestedACLs(fake *fakeAdminClient) []kafka.ACLEntry {
	fake.mu.Lock()
	defer fake.mu.Unlock()

	entries := make([]kafka.ACLEntry, 0, 8)
	for _, request := range fake.createACLsRequests {
		entries = append(entries, request.ACLs...)
	}

	return entries
}

// TestEnsureTopics_CreatesTheEightTopicsWithTheConfiguredGeometry pins the inventory and
// the geometry of a first run against an empty broker.
//
// The eight names are asserted exactly and in order. Topic naming has no runtime failure
// mode — a wrong name is a valid topic nobody reads — so an assertion that merely counted
// eight creations, or checked that the names were non-empty, would pass while the whole
// pipeline published into the void.
func TestEnsureTopics_CreatesTheEightTopicsWithTheConfiguredGeometry(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 3)

	report, err := admin.EnsureTopics(context.Background())
	require.NoError(t, err, "assuring topics against an empty broker must succeed")

	configs := createdTopicConfigs(fake)
	require.Len(t, configs, len(expectedEventTopics),
		"exactly the eight owned topics must be created: the four categories and their four dead-letter siblings")

	for index, expected := range expectedEventTopics {
		assert.Equal(t, expected, configs[index].Topic,
			"topic %d must be %q, in the canonical provisioning order", index, expected)
		assert.Equal(t, MinTopicPartitions, configs[index].NumPartitions,
			"topic %q must be created with the configured partition count", expected)
		assert.Equal(t, 3, configs[index].ReplicationFactor,
			"topic %q must be created with the configured replication factor", expected)
	}

	assert.Equal(t, len(expectedEventTopics), report.CreatedCount, "every topic was created on a first run")
	assert.Zero(t, report.GrownCount, "nothing existed, so nothing could be grown")
	assert.Zero(t, report.ShrinkRefusedCount, "nothing existed, so nothing could be over-partitioned")
	assert.Equal(t, expectedEventTopics, report.TopicNames(), "the report must cover the inventory in order")
	assert.Zero(t, fake.callCount("CreatePartitions"),
		"a freshly created topic already has the configured partition count and must not be grown")
}

// TestEnsureTopics_UsesTheConfiguredReplicationFactorOfOne is the anti-hard-coding test.
//
// A replication factor of 3 cannot be satisfied by a single-broker KRaft cluster, which
// rejects the creation outright, so a literal 3 anywhere in the creation path makes local
// bring-up impossible. A literal 1 would be worse in the other direction: it would
// silently discard the durability requirement in production. The only correct behaviour is
// to pass configuration through untouched, and this asserts a 1 arrives as a 1.
func TestEnsureTopics_UsesTheConfiguredReplicationFactorOfOne(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.EnsureTopics(context.Background())
	require.NoError(t, err, "a single-broker replication factor must be usable")

	configs := createdTopicConfigs(fake)
	require.NotEmpty(t, configs, "topics must have been created")
	for _, config := range configs {
		assert.Equal(t, 1, config.ReplicationFactor,
			"topic %q must be created with the configured factor of 1, never a hard-coded 3", config.Topic)
	}

	assert.Equal(t, 1, report.ReplicationFactor, "the report must state the factor that was applied")
}

// TestEnsureTopics_HonoursAPartitionCountAboveTheMinimum proves the floor is a floor and
// not a fixed value.
func TestEnsureTopics_HonoursAPartitionCountAboveTheMinimum(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	const configured = 12

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, configured, 1)

	report, err := admin.EnsureTopics(context.Background())
	require.NoError(t, err)

	for _, config := range createdTopicConfigs(fake) {
		assert.Equal(t, configured, config.NumPartitions,
			"a partition count above the minimum must be used as configured")
	}
	assert.Equal(t, configured, report.Partitions)
}

// TestEnsureTopics_IsIdempotentAcrossRuns is the property that lets topic assurance run on
// every start-up rather than behind a first-deploy-only flag.
//
// The second run must neither fail on "already exists" nor issue redundant work. Both
// halves matter: failing would break every restart, and re-issuing creations would make
// the operation noisy enough that operators would stop running it.
func TestEnsureTopics_IsIdempotentAcrossRuns(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	first, err := admin.EnsureTopics(context.Background())
	require.NoError(t, err, "the first run must create the inventory")
	require.Equal(t, len(expectedEventTopics), first.CreatedCount)

	creationsAfterFirstRun := fake.callCount("CreateTopics")

	second, err := admin.EnsureTopics(context.Background())
	require.NoError(t, err, "a second run must succeed rather than failing on topics that already exist")

	assert.Zero(t, second.CreatedCount, "nothing is left to create on a second run")
	assert.Zero(t, second.GrownCount, "the topics already have the configured partition count")
	assert.Equal(t, len(expectedEventTopics), second.UnchangedCount,
		"every topic must be reported as unchanged")
	assert.Equal(t, creationsAfterFirstRun, fake.callCount("CreateTopics"),
		"a second run must not send a creation request for topics that already exist")
	assert.Zero(t, fake.callCount("CreatePartitions"),
		"a topic already at the configured partition count must not be grown")
}

// TestEnsureTopics_GrowsAnUnderPartitionedTopic covers the single-partition topic an
// operator created by hand, or that a permissive broker auto-created.
//
// Growing matters because the partition count caps how far a subscriber's consumer group
// can scale: a one-partition topic silently limits it to one useful member.
func TestEnsureTopics_GrowsAnUnderPartitionedTopic(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	const undersized = "blnk.transactions"

	fake := newFakeAdminClient().withTopic(undersized, 1)
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.EnsureTopics(context.Background())
	require.NoError(t, err)

	growths := grownTopicConfigs(fake)
	require.Len(t, growths, 1, "only the under-partitioned topic may be grown")
	assert.Equal(t, undersized, growths[0].Name)
	assert.Equal(t, int32(MinTopicPartitions), growths[0].Count,
		"Kafka's CreatePartitions takes the new TOTAL count, not a delta")

	assurance, found := report.Lookup(undersized)
	require.True(t, found, "the grown topic must appear in the report")
	assert.True(t, assurance.PartitionsAdded, "the report must record the growth")
	assert.Equal(t, 1, assurance.PartitionsBefore)
	assert.Equal(t, MinTopicPartitions, assurance.PartitionsAfter)
	assert.False(t, assurance.Created, "an existing topic is not created")
	assert.Equal(t, 1, report.GrownCount)

	// The other seven were absent and therefore created, not grown.
	assert.Equal(t, len(expectedEventTopics)-1, report.CreatedCount)
}

// TestEnsureTopics_TreatsAConcurrentCreationAsSuccess covers two server instances starting
// at the same moment: the metadata probe says the topic is absent, and the creation loses
// the race.
//
// TOPIC_ALREADY_EXISTS must be success, and the topic must then be re-probed so its real
// partition count is known — otherwise a topic another provisioner created with one
// partition would never be grown.
func TestEnsureTopics_TreatsAConcurrentCreationAsSuccess(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	const raced = "blnk.balances"

	fake := newFakeAdminClient()
	fake.createTopicErrors[raced] = kafka.TopicAlreadyExists
	fake.racedPartitions = 2 // the winner created it under-partitioned

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.EnsureTopics(context.Background())
	require.NoError(t, err, "losing a creation race must not be an error")

	assurance, found := report.Lookup(raced)
	require.True(t, found)
	assert.False(t, assurance.Created, "the topic was created by the other provisioner, not by this run")
	assert.True(t, assurance.PartitionsAdded,
		"a topic another provisioner created under-partitioned must still be grown")
	assert.Equal(t, 2, assurance.PartitionsBefore)
	assert.Equal(t, MinTopicPartitions, assurance.PartitionsAfter)

	assert.GreaterOrEqual(t, fake.callCount("Metadata"), 2,
		"the raced topic must be re-probed so its real partition count is known")

	growths := grownTopicConfigs(fake)
	require.Len(t, growths, 1)
	assert.Equal(t, raced, growths[0].Name)
}

// TestEnsureTopics_RefusesToShrinkAnOverPartitionedTopic pins the deliberate refusal.
//
// Kafka cannot reduce a partition count, so attempting it could only fail; but the deeper
// reason is that shrinking would be WRONG even if it were possible, because the partition
// a key hashes to depends on the partition count. Reducing it would move a ledger's events
// to a different partition from their predecessors and break the per-aggregate ordering
// guarantee.
func TestEnsureTopics_RefusesToShrinkAnOverPartitionedTopic(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	const oversized = "blnk.identities"

	fake := newFakeAdminClient().withTopic(oversized, 12)
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.EnsureTopics(context.Background())
	require.NoError(t, err, "an over-partitioned topic is reported, not an error")

	assurance, found := report.Lookup(oversized)
	require.True(t, found)
	assert.True(t, assurance.ShrinkRefused, "the refusal must be reported")
	assert.Equal(t, 12, assurance.PartitionsBefore)
	assert.Equal(t, 12, assurance.PartitionsAfter, "the topic must be left exactly as it was")
	assert.False(t, assurance.PartitionsAdded)
	assert.Equal(t, 1, report.ShrinkRefusedCount)

	for _, growth := range grownTopicConfigs(fake) {
		assert.NotEqual(t, oversized, growth.Name,
			"no partition change may be attempted on an over-partitioned topic")
	}
}

// TestEnsureTopics_RefusesAnUnconfiguredReplicationFactor proves the factor is never
// guessed.
//
// Refusing loudly is the only safe behaviour: defaulting to 1 would discard durability in
// production without a word, and defaulting to 3 would make a single-broker stack fail at
// creation with an error that does not name the cause.
func TestEnsureTopics_RefusesAnUnconfiguredReplicationFactor(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 0)

	_, err := admin.EnsureTopics(context.Background())
	require.Error(t, err, "an unconfigured replication factor must be refused")
	assert.Contains(t, err.Error(), "KAFKA_REPLICATION_FACTOR",
		"the error must name the variable the operator has to set")
	assert.Zero(t, fake.totalCalls(), "nothing may be sent to the broker when the factor is unusable")
}

// TestEnsureTopics_ExplainsAnInvalidReplicationFactor turns the broker's terse error into
// an actionable one, because this is the single most likely first-run failure on a
// single-broker stack.
func TestEnsureTopics_ExplainsAnInvalidReplicationFactor(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	fake := newFakeAdminClient()
	fake.createTopicErrors["blnk.transactions"] = kafka.InvalidReplicationFactor

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 3)

	_, err := admin.EnsureTopics(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KAFKA_REPLICATION_FACTOR",
		"the error must tell the operator which variable to change")
	assert.Contains(t, err.Error(), "blnk.transactions", "the error must name the topic that failed")
	assert.ErrorIs(t, err, kafka.InvalidReplicationFactor, "the broker's own error must remain inspectable")
}

// TestEnsureTopics_SurfacesAMetadataAuthorizationFailure proves a permission problem is not
// mistaken for an absent topic.
//
// Treating TOPIC_AUTHORIZATION_FAILED as "not there" would make the operation try to create
// a topic that exists, and then report success while the administrative principal was in
// fact unable to see anything.
func TestEnsureTopics_SurfacesAMetadataAuthorizationFailure(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	fake := newFakeAdminClient()
	fake.metadataTopicErrors["blnk.system"] = kafka.TopicAuthorizationFailed

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	_, err := admin.EnsureTopics(context.Background())
	require.Error(t, err, "an authorization failure must not be read as an absent topic")
	assert.ErrorIs(t, err, kafka.TopicAuthorizationFailed)
	assert.Zero(t, fake.callCount("CreateTopics"), "no creation may be attempted on an unreadable cluster")
}

// TestEnsureTopics_ReportsNoGrowthWhenTheBrokerRefusesIt keeps the report honest.
//
// A report is returned alongside the error so a caller can see how far assurance got, and
// it must never claim a change the broker did not make: an entry says PartitionsAdded only
// once CreatePartitions has confirmed it.
func TestEnsureTopics_ReportsNoGrowthWhenTheBrokerRefusesIt(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	const undersized = "blnk.transactions"

	fake := newFakeAdminClient().withTopic(undersized, 1)
	fake.transportErrors["CreatePartitions"] = errors.New("broker unreachable")

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.EnsureTopics(context.Background())
	require.Error(t, err, "a failed growth must be reported as an error")

	assurance, found := report.Lookup(undersized)
	require.True(t, found, "the report must still describe the topic")
	assert.False(t, assurance.PartitionsAdded, "a growth that did not happen must not be claimed")
	assert.Equal(t, 1, assurance.PartitionsAfter, "the report must describe the topic as it still stands")
	assert.Zero(t, report.GrownCount)
}

// TestResolveTopicPartitions_AppliesTheRequiredFloorAndACeiling covers the geometry
// normalisation in isolation.
//
// The floor of six is a requirement rather than a preference, so a lower configured value
// is raised rather than honoured. The ceiling exists so that a pasted number cannot wrap
// the int32 conversion the create and grow requests need, where a negative count would be
// read by Kafka as "unset" and silently answered with the broker default.
func TestResolveTopicPartitions_AppliesTheRequiredFloorAndACeiling(t *testing.T) {
	cases := []struct {
		name       string
		configured int
		expected   int
	}{
		{name: "unset", configured: 0, expected: MinTopicPartitions},
		{name: "negative", configured: -4, expected: MinTopicPartitions},
		{name: "below the floor", configured: 2, expected: MinTopicPartitions},
		{name: "exactly the floor", configured: MinTopicPartitions, expected: MinTopicPartitions},
		{name: "above the floor", configured: 24, expected: 24},
		{name: "implausible", configured: maxTopicPartitions + 1, expected: maxTopicPartitions},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			assert.Equal(t, testCase.expected, resolveTopicPartitions(testCase.configured))
		})
	}
}

// sentinelPassword is a value that could not occur by accident, so any appearance of it in
// a log line, an error or a serialised result is proof that the secret escaped.
//
// It is a fake credential, not a real one, and it is printable ASCII so it passes the
// derivation's own input rule.
const sentinelPassword = "Sentinel-Do-Not-Log-9f3c1a7e"

// testSubscriber returns a registry row with a realistic access boundary.
func testSubscriber() *model.EventSubscriber {
	prefix := "acme-"

	return &model.EventSubscriber{
		SubscriberID:       "sub_0f6e2c8a",
		Name:               "Acme Reconciliation",
		KafkaPrincipal:     "acme-recon",
		ConsumerGroupID:    "acme-recon-group",
		AuthorizedTopics:   []string{"blnk.transactions", "blnk.balances"},
		PartitionKeyPrefix: &prefix,
	}
}

// TestProvisionSubscriberPrincipal_UsesSha512WithAtLeastTheMinimumIterations pins the
// credential derivation.
//
// Kafka implements only SCRAM-SHA-256 and SCRAM-SHA-512 and enforces a 4096-iteration
// minimum, and its AlterUserScramCredentials API takes a SALT and a SALTED PASSWORD rather
// than a plaintext one. Every one of those properties is asserted here, including that the
// transmitted bytes are a derivation and not the password itself.
func TestProvisionSubscriberPrincipal_UsesSha512WithAtLeastTheMinimumIterations(t *testing.T) {
	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	result, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword),
	)
	require.NoError(t, err)

	fake.mu.Lock()
	requests := fake.scramUpsertRequests
	fake.mu.Unlock()

	require.Len(t, requests, 1, "exactly one credential must be written")
	require.Len(t, requests[0].Upsertions, 1, "exactly one principal must be upserted")
	require.Empty(t, requests[0].Deletions, "provisioning must never delete a credential")

	upsertion := requests[0].Upsertions[0]
	assert.Equal(t, "acme-recon", upsertion.Name, "the credential must belong to the subscriber's principal")
	assert.Equal(t, kafka.ScramMechanismSha512, upsertion.Mechanism,
		"Blnk standardises on SCRAM-SHA-512; SHA-256 cannot authenticate that mechanism")
	assert.GreaterOrEqual(t, upsertion.Iterations, MinScramIterations,
		"the iteration count must be at least the minimum Kafka accepts")
	assert.Equal(t, DefaultScramIterations, upsertion.Iterations,
		"an unspecified iteration count must take the documented default")

	assert.Len(t, upsertion.Salt, scramSaltLength, "a per-credential random salt must be generated")
	assert.Len(t, upsertion.SaltedPassword, 64,
		"a SHA-512 derivation is 64 bytes; a shorter value means the wrong hash was used")
	assert.NotEqual(t, []byte(sentinelPassword), upsertion.SaltedPassword,
		"the plaintext password must never be sent to the broker")
	assert.NotContains(t, string(upsertion.SaltedPassword), sentinelPassword,
		"the derivation must not embed the plaintext password")

	assert.Equal(t, SubscriberSASLMechanism, result.Mechanism)
	assert.Equal(t, DefaultScramIterations, result.Iterations)
	assert.Equal(t, "acme-recon", result.Principal)
	assert.False(t, result.CredentialReplaced, "no credential existed beforehand")
	assert.True(t, result.AuthorizerActive, "the modelled broker enforces ACLs")
	assert.False(t, result.ProvisionedAt.IsZero(), "the result must record when provisioning completed")
}

// TestProvisionSubscriberPrincipal_SaltIsFreshPerCredential proves two issuances do not
// share a salt, which is the whole point of salting.
func TestProvisionSubscriberPrincipal_SaltIsFreshPerCredential(t *testing.T) {
	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)
	request := NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword)

	_, err := admin.ProvisionSubscriberPrincipal(context.Background(), request)
	require.NoError(t, err)
	_, err = admin.ProvisionSubscriberPrincipal(context.Background(), request)
	require.NoError(t, err)

	fake.mu.Lock()
	requests := fake.scramUpsertRequests
	fake.mu.Unlock()

	require.Len(t, requests, 2)
	assert.NotEqual(t, requests[0].Upsertions[0].Salt, requests[1].Upsertions[0].Salt,
		"each issuance must generate its own salt")
	assert.NotEqual(t, requests[0].Upsertions[0].SaltedPassword, requests[1].Upsertions[0].SaltedPassword,
		"the same password with a different salt must derive a different credential")
}

// TestProvisionSubscriberPrincipal_ClampsIterationsToTheKafkaMinimum covers the request
// that asks for too few rounds.
//
// Raising rather than forwarding matters because the broker's own rejection arrives as
// UNACCEPTABLE_CREDENTIAL, which reads like a bad password rather than a bad parameter.
func TestProvisionSubscriberPrincipal_ClampsIterationsToTheKafkaMinimum(t *testing.T) {
	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	request := NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword)
	request.Iterations = 512

	result, err := admin.ProvisionSubscriberPrincipal(context.Background(), request)
	require.NoError(t, err)

	fake.mu.Lock()
	upsertion := fake.scramUpsertRequests[0].Upsertions[0]
	fake.mu.Unlock()

	assert.Equal(t, MinScramIterations, upsertion.Iterations,
		"an iteration count below the Kafka minimum must be raised to it")
	assert.Equal(t, MinScramIterations, result.Iterations, "the result must report the count actually used")
}

// TestProvisionSubscriberPrincipal_HonoursAStrongerIterationCount proves the minimum is a
// floor and not a fixed value, so a deployment may harden its credentials.
func TestProvisionSubscriberPrincipal_HonoursAStrongerIterationCount(t *testing.T) {
	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	request := NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword)
	request.Iterations = 16384

	_, err := admin.ProvisionSubscriberPrincipal(context.Background(), request)
	require.NoError(t, err)

	fake.mu.Lock()
	upsertion := fake.scramUpsertRequests[0].Upsertions[0]
	fake.mu.Unlock()

	assert.Equal(t, 16384, upsertion.Iterations, "a stronger iteration count must be honoured")
}

// TestProvisionSubscriberPrincipal_BindsLeastPrivilegeACLs asserts the access boundary
// binding by binding.
//
// This is the construction behind the subscriber-isolation criterion. Every field is
// checked because every field can silently widen the grant: a PREFIXED pattern on a topic
// would hand over every topic sharing the prefix, and a missing "User:" prefix produces a
// binding that is accepted and never matches, so the grant looks present while every
// request is denied.
func TestProvisionSubscriberPrincipal_BindsLeastPrivilegeACLs(t *testing.T) {
	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	_, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword),
	)
	require.NoError(t, err)

	entries := requestedACLs(fake)
	require.Len(t, entries, 5,
		"two topics need Read and Describe each, plus one consumer-group binding")

	expected := []kafka.ACLEntry{
		{
			ResourceType:        kafka.ResourceTypeTopic,
			ResourceName:        "blnk.transactions",
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           "User:acme-recon",
			Host:                ACLHostAny,
			Operation:           kafka.ACLOperationTypeRead,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		},
		{
			ResourceType:        kafka.ResourceTypeTopic,
			ResourceName:        "blnk.transactions",
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           "User:acme-recon",
			Host:                ACLHostAny,
			Operation:           kafka.ACLOperationTypeDescribe,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		},
		{
			ResourceType:        kafka.ResourceTypeTopic,
			ResourceName:        "blnk.balances",
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           "User:acme-recon",
			Host:                ACLHostAny,
			Operation:           kafka.ACLOperationTypeRead,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		},
		{
			ResourceType:        kafka.ResourceTypeTopic,
			ResourceName:        "blnk.balances",
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           "User:acme-recon",
			Host:                ACLHostAny,
			Operation:           kafka.ACLOperationTypeDescribe,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		},
		{
			ResourceType:        kafka.ResourceTypeGroup,
			ResourceName:        "acme-recon-group",
			ResourcePatternType: kafka.PatternTypePrefixed,
			Principal:           "User:acme-recon",
			Host:                ACLHostAny,
			Operation:           kafka.ACLOperationTypeRead,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		},
	}

	assert.Equal(t, expected, entries,
		"the bindings must be exactly Read and Describe on each literal topic plus Read on the prefixed group")
}

// TestProvisionSubscriberPrincipal_NeverGrantsWriteOrAWildcardPattern is the negative half
// of the isolation criterion, and it is deliberately written as a scan over whatever
// bindings were produced rather than as an equality check.
//
// An equality assertion proves what today's grant is. This proves what no grant may ever
// become, so a future widening fails here even if the equality test above was updated to
// match it.
func TestProvisionSubscriberPrincipal_NeverGrantsWriteOrAWildcardPattern(t *testing.T) {
	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	subscriber := testSubscriber()
	subscriber.AuthorizedTopics = expectedEventTopics // the widest legitimate grant

	_, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
	)
	require.NoError(t, err)

	entries := requestedACLs(fake)
	require.NotEmpty(t, entries)

	forbiddenOperations := map[kafka.ACLOperationType]string{
		kafka.ACLOperationTypeWrite:           "a subscriber must never be able to publish",
		kafka.ACLOperationTypeCreate:          "a subscriber must never be able to create resources",
		kafka.ACLOperationTypeDelete:          "a subscriber must never be able to delete resources",
		kafka.ACLOperationTypeAlter:           "a subscriber must never be able to alter resources",
		kafka.ACLOperationTypeAlterConfigs:    "a subscriber must never be able to alter configuration",
		kafka.ACLOperationTypeAll:             "an All grant is the opposite of least privilege",
		kafka.ACLOperationTypeAny:             "an Any grant is not a grant, it is a hole",
		kafka.ACLOperationTypeClusterAction:   "cluster actions are administrative",
		kafka.ACLOperationTypeIdempotentWrite: "a subscriber must never be able to publish",
	}

	for _, entry := range entries {
		reason, forbidden := forbiddenOperations[entry.Operation]
		assert.False(t, forbidden, "binding %v on %q: %s", entry.Operation, entry.ResourceName, reason)

		assert.Equal(t, kafka.ACLPermissionTypeAllow, entry.PermissionType,
			"every binding must be an explicit Allow; a Deny here would be a different security model")
		assert.NotEqual(t, kafka.ResourceTypeCluster, entry.ResourceType,
			"cluster-wide grants would let a subscriber describe the whole cluster")
		assert.NotEqual(t, "*", entry.ResourceName,
			"a wildcard resource name would grant every topic in the cluster")
		assert.NotEmpty(t, entry.ResourceName, "an empty resource name is not a boundary")
		assert.True(t, strings.HasPrefix(entry.Principal, "User:"),
			"a principal without the User: prefix is accepted by Kafka and then never matches")

		if entry.ResourceType == kafka.ResourceTypeTopic {
			assert.Equal(t, kafka.PatternTypeLiteral, entry.ResourcePatternType,
				"topic %q must be granted literally; a prefixed pattern would widen the grant to every "+
					"topic sharing the prefix", entry.ResourceName)
		}
	}
}

// TestProvisionSubscriberPrincipal_NeverLeaksThePassword is the secret-handling assertion,
// exercised over the whole successful path and over a failing one.
//
// It checks all four escape routes at once: the log message, the structured log fields, the
// serialised result and the returned error.
func TestProvisionSubscriberPrincipal_NeverLeaksThePassword(t *testing.T) {
	t.Run("successful provisioning", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		fake := newFakeAdminClient()
		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		result, err := admin.ProvisionSubscriberPrincipal(
			context.Background(),
			NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword),
		)
		require.NoError(t, err)

		assertNoPasswordInLogs(t, hook)

		serialised, marshalErr := json.Marshal(result)
		require.NoError(t, marshalErr)
		assert.NotContains(t, string(serialised), sentinelPassword,
			"the result is logged and may be serialised, so it must carry no secret")
	})

	t.Run("broker rejects the credential", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		fake := newFakeAdminClient()
		fake.scramUpsertError = kafka.UnacceptableCredential
		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		result, err := admin.ProvisionSubscriberPrincipal(
			context.Background(),
			NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword),
		)
		require.Error(t, err)
		assert.NotContains(t, err.Error(), sentinelPassword,
			"an error about a credential must never quote the credential")
		assert.ErrorIs(t, err, kafka.UnacceptableCredential, "the broker's own error must stay inspectable")
		assertNoPasswordInLogs(t, hook)

		assert.Zero(t, len(requestedACLs(fake)),
			"no boundary may be bound for a credential the broker refused")
		assert.False(t, result.ProvisionedAt.IsZero() && result.ACLBindings > 0)
	})

	t.Run("rejected before any request", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		fake := newFakeAdminClient()
		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		request := NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword+"\n")

		_, err := admin.ProvisionSubscriberPrincipal(context.Background(), request)
		require.Error(t, err, "a password outside printable ASCII must be refused")
		assert.NotContains(t, err.Error(), sentinelPassword,
			"the rejection must describe the rule, never the value")
		assert.Zero(t, fake.totalCalls(), "an unusable password must not reach the broker")
		assertNoPasswordInLogs(t, hook)
	})
}

// assertNoPasswordInLogs fails when the sentinel appears anywhere in the captured log
// entries, message or structured field.
func assertNoPasswordInLogs(t *testing.T, hook *logtest.Hook) {
	t.Helper()

	for _, entry := range hook.AllEntries() {
		assert.NotContains(t, entry.Message, sentinelPassword,
			"a log message must never contain the SCRAM password")

		for key, value := range entry.Data {
			rendered := strings.TrimSpace(strings.Join([]string{key, format(value)}, "="))
			assert.NotContains(t, rendered, sentinelPassword,
				"log field %q must never contain the SCRAM password", key)
		}
	}
}

// format renders a structured log field value for substring inspection.
func format(value interface{}) string {
	if err, ok := value.(error); ok {
		return err.Error()
	}

	encoded, marshalErr := json.Marshal(value)
	if marshalErr != nil {
		return ""
	}

	return string(encoded)
}

// TestValidateSCRAMPassword_RejectsAnythingOutsidePrintableASCII covers the restriction in
// isolation.
//
// The restriction is a correctness requirement, not a policy preference: a SCRAM client
// SASLpreps the password before proving knowledge of it, while the broker stores what is
// derived here. The two provably agree only on printable ASCII, and outside that range the
// result is a credential that authenticates for nobody while failing exactly like a wrong
// password.
func TestValidateSCRAMPassword_RejectsAnythingOutsidePrintableASCII(t *testing.T) {
	valid := []string{
		"Sentinel-Do-Not-Log-9f3c1a7e",
		"aB3!@#$%^&*()_+-=[]{}|;:',.<>/?`~\"\\",
		"0123456789",
	}
	for _, password := range valid {
		assert.NoError(t, validateSCRAMPassword(password),
			"printable ASCII must be accepted: %q", password)
	}

	// The values are deliberately unlike any English word: a password that happened to be a
	// substring of the rejection message would make the "must not echo the value" assertion
	// below fail for a reason that has nothing to do with the code.
	invalid := map[string]string{
		"empty":          "",
		"trailing space": "Xk9qZ2 ",
		"leading space":  " Xk9qZ2",
		"newline":        "Xk9qZ2\n",
		"tab":            "Xk9q\tZ2",
		"null byte":      "Xk9qZ2\x00",
		"non-ascii":      "Xk9qZé2",
		"delete":         "Xk9qZ2\x7f",
	}
	for name, password := range invalid {
		t.Run(name, func(t *testing.T) {
			err := validateSCRAMPassword(password)
			require.Error(t, err, "%s must be refused", name)
			if password != "" {
				assert.NotContains(t, err.Error(), password,
					"the rejection must not echo the value it rejected")
			}
		})
	}
}

// TestProvisionSubscriberPrincipal_RejectsAnEmptyPrincipal covers the subscriber row that
// was never given a Kafka principal.
func TestProvisionSubscriberPrincipal_RejectsAnEmptyPrincipal(t *testing.T) {
	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	subscriber := testSubscriber()
	subscriber.KafkaPrincipal = "   "

	_, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "kafka_principal", "the error must name the field that is missing")
	assert.Zero(t, fake.totalCalls(), "nothing may be sent without a principal to bind it to")
}

// TestProvisionSubscriberPrincipal_ReportsAReplacedCredential proves re-issuing is a
// well-defined operation rather than an error.
//
// It has to be: a subscriber that lost its secret can only be given a new one, and an
// operator needs to know from the result that a working consumer's credential has just
// stopped working.
func TestProvisionSubscriberPrincipal_ReportsAReplacedCredential(t *testing.T) {
	fake := newFakeAdminClient()
	fake.scram["acme-recon"] = []kafka.ScramMechanism{kafka.ScramMechanismSha512}

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	result, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword),
	)
	require.NoError(t, err, "re-issuing a credential must not be an error")
	assert.True(t, result.CredentialReplaced, "replacing an existing credential must be reported")
	assert.Equal(t, 1, fake.callCount("AlterUserScramCredentials"), "the credential must be upserted")
}

// TestProvisionSubscriberPrincipal_WarnsWhenTheBrokerEnforcesNothing is the guard against a
// vacuous isolation guarantee.
//
// A KRaft broker without an authorizer accepts every binding and applies none. Provisioning
// still succeeds — refusing would make Blnk unusable against such a broker — but the
// finding must be reported and shouted about, because the alternative is a security
// property that reports itself satisfied while absent.
func TestProvisionSubscriberPrincipal_WarnsWhenTheBrokerEnforcesNothing(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	fake := newFakeAdminClient()
	fake.securityDisabled = true

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	result, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword),
	)
	require.NoError(t, err, "an unenforced broker must not block provisioning")
	assert.False(t, result.AuthorizerActive, "the finding must be reported to the caller")

	var shouted bool
	for _, entry := range hook.AllEntries() {
		if entry.Level <= logrus.ErrorLevel &&
			strings.Contains(entry.Message, "StandardAuthorizer") {
			shouted = true

			break
		}
	}
	assert.True(t, shouted,
		"a broker that enforces no ACLs must produce a prominent log entry naming the authorizer to configure")
}

// TestProvisionSubscriberPrincipal_WarnsWhenTheGrantIsEmpty covers the fail-closed registry
// row: a subscriber authorised for nothing.
//
// It is a legitimate state, so it is provisioned rather than refused, but silence would let
// an operator believe a consumer was ready when it can read nothing at all.
func TestProvisionSubscriberPrincipal_WarnsWhenTheGrantIsEmpty(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	subscriber := testSubscriber()
	subscriber.AuthorizedTopics = nil
	subscriber.ConsumerGroupID = ""

	result, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
	)
	require.NoError(t, err, "an empty grant is fail-closed, not invalid")
	assert.Zero(t, result.ACLBindings, "there is nothing to bind")
	assert.Zero(t, fake.callCount("CreateACLs"), "no binding request may be sent when there is no grant")

	warnings := 0
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, "provisioned") {
			warnings++
		}
	}
	assert.GreaterOrEqual(t, warnings, 2,
		"both the absent topic grant and the absent consumer group must be reported")
}

// TestNewSubscriberProvisioningRequest_MapsTheRegistryRowAndNotThePartitionKeyPrefix pins
// the mapping, including the one field that is deliberately NOT mapped.
//
// Kafka's authorizer has no message-key dimension, so the partition-key prefix cannot be
// enforced by any ACL. Expressing it as a PREFIXED topic pattern — the only binding that
// looks like it might fit — would WIDEN the topic grant to every topic sharing the prefix
// while appearing to narrow it, which is strictly worse than not enforcing it at all.
func TestNewSubscriberProvisioningRequest_MapsTheRegistryRowAndNotThePartitionKeyPrefix(t *testing.T) {
	subscriber := testSubscriber()

	request := NewSubscriberProvisioningRequest(subscriber, sentinelPassword)

	assert.Equal(t, subscriber.SubscriberID, request.SubscriberID)
	assert.Equal(t, subscriber.KafkaPrincipal, request.Principal)
	assert.Equal(t, subscriber.ConsumerGroupID, request.ConsumerGroupPrefix)
	assert.Equal(t, subscriber.AuthorizedTopics, request.Topics)
	assert.Equal(t, sentinelPassword, request.Password, "the password must be carried through unchanged")

	for _, entry := range request.aclEntries() {
		if entry.ResourceType != kafka.ResourceTypeTopic {
			continue
		}
		assert.NotEqual(t, *subscriber.PartitionKeyPrefix, entry.ResourceName,
			"the partition-key prefix must never become a topic resource name")
		assert.Equal(t, kafka.PatternTypeLiteral, entry.ResourcePatternType,
			"the partition-key prefix must never be expressed as a prefixed topic pattern")
	}

	assert.Equal(t, SubscriberProvisioningRequest{Password: sentinelPassword},
		NewSubscriberProvisioningRequest(nil, sentinelPassword),
		"a nil registry row must produce a request that fails validation rather than a panic")
}

// TestSubscriberProvisioningRequest_NormalisesItsInputs covers the small normalisations the
// bindings depend on.
func TestSubscriberProvisioningRequest_NormalisesItsInputs(t *testing.T) {
	request := SubscriberProvisioningRequest{
		Principal:           "  acme-recon  ",
		ConsumerGroupPrefix: "  acme-group ",
		Topics:              []string{" blnk.transactions ", "", "blnk.transactions", "blnk.balances"},
	}

	assert.Equal(t, "acme-recon", request.principal())
	assert.Equal(t, "acme-group", request.consumerGroupPrefix())
	assert.Equal(t, []string{"blnk.transactions", "blnk.balances"}, request.normalizedTopics(),
		"blanks and duplicates must be dropped while the registry's order is preserved")
	assert.Equal(t, ACLHostAny, request.host(), "an unset host must widen to every host, not to none")

	request.Host = " 10.0.0.7 "
	assert.Equal(t, "10.0.0.7", request.host(), "a configured host must be honoured")
}

// TestSubscriberCredentialExists_DistinguishesMechanismAndAbsence covers the three answers
// the probe has to give.
//
// A SHA-256-only principal answers false, because a SHA-256 credential cannot authenticate
// the SHA-512 mechanism Blnk standardises on: for Blnk's purposes no credential exists.
func TestSubscriberCredentialExists_DistinguishesMechanismAndAbsence(t *testing.T) {
	fake := newFakeAdminClient()
	fake.scram["has-sha512"] = []kafka.ScramMechanism{kafka.ScramMechanismSha512}
	fake.scram["has-sha256"] = []kafka.ScramMechanism{kafka.ScramMechanismSha256}

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	exists, err := admin.SubscriberCredentialExists(context.Background(), "has-sha512")
	require.NoError(t, err)
	assert.True(t, exists, "a SHA-512 credential must be reported as present")

	exists, err = admin.SubscriberCredentialExists(context.Background(), "has-sha256")
	require.NoError(t, err)
	assert.False(t, exists, "a SHA-256-only credential cannot authenticate SCRAM-SHA-512")

	exists, err = admin.SubscriberCredentialExists(context.Background(), "never-provisioned")
	require.NoError(t, err, "an unknown principal is an answer, not an error")
	assert.False(t, exists)

	_, err = admin.SubscriberCredentialExists(context.Background(), "  ")
	require.Error(t, err, "a blank principal is a programming error and must be refused")
}

// TestAuthorizerActive_DistinguishesDisabledFromUnanswerable is the distinction that keeps
// the isolation criterion meaningful.
//
// "Security is disabled" means nobody is being checked. "I am not allowed to ask" means
// something quite different, and reporting the second as the first would tell an operator
// their ACLs are unenforced when they are merely unreadable.
func TestAuthorizerActive_DistinguishesDisabledFromUnanswerable(t *testing.T) {
	t.Run("enforcing", func(t *testing.T) {
		admin := newTestKafkaAdmin(newFakeAdminClient(), MinTopicPartitions, 1)

		active, err := admin.AuthorizerActive(context.Background())
		require.NoError(t, err)
		assert.True(t, active)
	})

	t.Run("security disabled", func(t *testing.T) {
		fake := newFakeAdminClient()
		fake.securityDisabled = true
		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		active, err := admin.AuthorizerActive(context.Background())
		require.NoError(t, err, "an explicit SECURITY_DISABLED answer is an answer, not a failure")
		assert.False(t, active)
	})

	t.Run("unanswerable", func(t *testing.T) {
		fake := newFakeAdminClient()
		fake.transportErrors["DescribeACLs"] = errors.New("broker unreachable")
		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		active, err := admin.AuthorizerActive(context.Background())
		require.Error(t, err, "an unanswerable probe must not masquerade as an unenforced broker")
		assert.False(t, active)
	})

	t.Run("filter matches everything", func(t *testing.T) {
		fake := newFakeAdminClient()
		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		_, err := admin.AuthorizerActive(context.Background())
		require.NoError(t, err)

		fake.mu.Lock()
		requests := fake.describeACLsRequests
		fake.mu.Unlock()

		require.Len(t, requests, 1)
		filter := requests[0].Filter
		assert.Equal(t, kafka.PatternTypeAny, filter.ResourcePatternTypeFilter)
		assert.Equal(t, kafka.ACLOperationTypeAny, filter.Operation)
		assert.Equal(t, kafka.ACLPermissionTypeAny, filter.PermissionType)
		assert.Empty(t, filter.ResourceNameFilter, "an empty name filter is encoded as null and matches anything")
		assert.Empty(t, filter.PrincipalFilter)
		assert.Empty(t, filter.HostFilter)
	})
}

// TestLagForPartition_ComputesEveryCaseWithoutEverGoingNegative is the lag arithmetic
// table.
//
// Lag is the number the alert rule fires on, so its arithmetic is the highest-value thing
// in this file to pin. Each case below is a real broker state, not a synthetic input, and
// the last group is the one that matters most: a negative lag would pull a summed figure
// below the truth and silence the very alert this number exists to raise.
func TestLagForPartition_ComputesEveryCaseWithoutEverGoingNegative(t *testing.T) {
	cases := []struct {
		name      string
		committed int64
		first     int64
		end       int64
		expected  int64
	}{
		{name: "caught up exactly", committed: 5, first: 0, end: 5, expected: 0},
		{name: "one record behind", committed: 4, first: 0, end: 5, expected: 1},
		{name: "at the head of a full log", committed: 0, first: 0, end: 10, expected: 10},
		{name: "mid log", committed: 250, first: 0, end: 1000, expected: 750},
		{
			name:      "no commit on a full log is full lag",
			committed: -1, first: 0, end: 10, expected: 10,
		},
		{
			name:      "no commit after retention trimmed the head",
			committed: -1, first: 4, end: 10, expected: 6,
		},
		{
			name:      "no commit and an unknown first offset falls back to zero",
			committed: -1, first: -1, end: 10, expected: 10,
		},
		{
			name:      "no commit on an empty partition",
			committed: -1, first: 0, end: 0, expected: 0,
		},
		{name: "empty partition", committed: 0, first: 0, end: 0, expected: 0},
		{name: "unreadable end offset", committed: 3, first: 0, end: -1, expected: 0},
		{
			name:      "commit level with a retention-trimmed head",
			committed: 4, first: 4, end: 4, expected: 0,
		},
		{
			name:      "commit beyond the end offset is clamped, never negative",
			committed: 12, first: 0, end: 10, expected: 0,
		},
		{
			name:      "commit far beyond the end offset is clamped",
			committed: 1 << 40, first: 0, end: 10, expected: 0,
		},
		{
			name:      "large offsets do not overflow",
			committed: 1 << 40, first: 0, end: 1<<40 + 7, expected: 7,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			lag := lagForPartition(testCase.committed, testCase.first, testCase.end)

			assert.Equal(t, testCase.expected, lag)
			assert.GreaterOrEqual(t, lag, int64(0), "lag must never be negative")
		})
	}
}

// TestLagForPartition_IsNeverNegativeForAnyCombination sweeps the neighbourhood of every
// boundary rather than trusting the table above to have found them all.
//
// Negative values are included deliberately: -1 is the broker's own sentinel for "no
// committed offset" and for "unreadable", so the function has to be total over them.
func TestLagForPartition_IsNeverNegativeForAnyCombination(t *testing.T) {
	offsets := []int64{-2, -1, 0, 1, 2, 5, 10, 1 << 31, 1 << 40}

	for _, committed := range offsets {
		for _, first := range offsets {
			for _, end := range offsets {
				lag := lagForPartition(committed, first, end)

				require.GreaterOrEqual(t, lag, int64(0),
					"committed=%d first=%d end=%d produced a negative lag", committed, first, end)
				if end > 0 {
					require.LessOrEqual(t, lag, end,
						"committed=%d first=%d end=%d produced a lag beyond the end of the log",
						committed, first, end)
				}
			}
		}
	}
}

// TestConsumerLag_SumsPartitionsAndTopicsAndFeedsTheSharedGauge is the end-to-end lag path.
//
// It asserts the per-partition detail, the per-topic totals, the overall total and the gauge
// contract in one place, because those four have to agree: the alert reads the gauge, and an
// operator triaging the alert reads the detail.
func TestConsumerLag_SumsPartitionsAndTopicsAndFeedsTheSharedGauge(t *testing.T) {
	gauge := captureConsumerLagGauge(t)

	fake := newFakeAdminClient()
	// blnk.transactions: partition 0 is 40 behind, partition 1 is caught up.
	fake.withOffsets("blnk.transactions", 0, 0, 100).withCommitted("blnk.transactions", 0, 60)
	fake.withOffsets("blnk.transactions", 1, 0, 50).withCommitted("blnk.transactions", 1, 50)
	// blnk.balances: never committed on a log whose head was trimmed at 4.
	fake.withOffsets("blnk.balances", 0, 4, 10)

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{
		SubscriberID: "sub_0f6e2c8a",
		GroupID:      "acme-recon-group",
		Topics:       []string{"blnk.transactions", "blnk.balances"},
	})
	require.NoError(t, err)

	assert.Equal(t, int64(46), report.TotalLag, "40 behind on one partition plus 6 unread records")
	assert.Equal(t, map[string]int64{"blnk.transactions": 40, "blnk.balances": 6}, report.LagByTopic())
	require.Len(t, report.Topics, 2, "the report must keep the requested topic order")
	assert.Equal(t, "blnk.transactions", report.Topics[0].Topic)
	assert.Equal(t, "blnk.balances", report.Topics[1].Topic)
	assert.Empty(t, report.MissingTopics)
	assert.False(t, report.MeasuredAt.IsZero(), "the snapshot instant must be recorded")

	transactions := report.Topics[0]
	require.Len(t, transactions.Partitions, 2, "partitions must be reported in ascending order")
	assert.Equal(t, 0, transactions.Partitions[0].Partition)
	assert.Equal(t, int64(40), transactions.Partitions[0].Lag)
	assert.Equal(t, int64(60), transactions.Partitions[0].CommittedOffset)
	assert.Equal(t, int64(100), transactions.Partitions[0].EndOffset)
	assert.True(t, transactions.Partitions[0].Committed)
	assert.Equal(t, 1, transactions.Partitions[1].Partition)
	assert.Zero(t, transactions.Partitions[1].Lag)
	assert.Zero(t, transactions.PartitionsWithoutCommit)

	balances := report.Topics[1]
	require.Len(t, balances.Partitions, 1)
	assert.False(t, balances.Partitions[0].Committed, "the group has never committed here")
	assert.Equal(t, int64(-1), balances.Partitions[0].CommittedOffset,
		"the broker's own sentinel for an absent commit must be reported as-is")
	assert.Equal(t, int64(6), balances.Partitions[0].Lag)
	assert.Equal(t, 1, balances.PartitionsWithoutCommit)

	records := gauge.snapshot()
	require.Len(t, records, 2, "the gauge must be fed once per topic")
	for _, record := range records {
		assert.Equal(t, "sub_0f6e2c8a", record.attributes["subscriber"],
			"the alert rule interpolates $labels.subscriber")
		assert.Equal(t, "acme-recon-group", record.attributes["group"],
			"the alert rule interpolates $labels.group")
		assert.NotEmpty(t, record.attributes["topic"], "the alert rule interpolates $labels.topic")
		assert.GreaterOrEqual(t, record.value, int64(0), "no negative value may ever reach the gauge")
	}
	assert.Equal(t, int64(40), records[0].value)
	assert.Equal(t, int64(6), records[1].value)
}

// TestConsumerLag_TreatsAMissingCommitAsFullLagFromTheEarliestRetainedOffset pins the
// documented policy for a consumer group that has never committed.
//
// This is the single most important case the measurement has to catch: a subscriber that
// never started must NOT look healthy. Scoring it as zero would do exactly that, and
// scoring it from offset zero would invent lag for records retention has already deleted.
func TestConsumerLag_TreatsAMissingCommitAsFullLagFromTheEarliestRetainedOffset(t *testing.T) {
	fake := newFakeAdminClient()
	fake.withOffsets("blnk.transactions", 0, 1_000, 1_500)
	fake.groupNotFound = true // the consumer group has never existed

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{
		GroupID: "never-started",
		Topics:  []string{"blnk.transactions"},
	})
	require.NoError(t, err, "a consumer group that does not exist is a state, not an error")

	assert.Equal(t, int64(500), report.TotalLag,
		"lag must be measured from the earliest RETAINED offset, not from zero")
	require.Len(t, report.Topics, 1)
	assert.Equal(t, 1, report.Topics[0].PartitionsWithoutCommit)
}

// TestConsumerLag_ExcludesAnUnreadablePartitionRatherThanScoringItZero is the
// distinguishability requirement.
//
// A partition whose offsets cannot be read has unknown lag. Counting it as zero would make
// an unreadable partition indistinguishable from a healthy one, which is how a real backlog
// hides behind a green dashboard.
func TestConsumerLag_ExcludesAnUnreadablePartitionRatherThanScoringItZero(t *testing.T) {
	fake := newFakeAdminClient()
	fake.withOffsets("blnk.transactions", 0, 0, 100).withCommitted("blnk.transactions", 0, 90)
	fake.withOffsets("blnk.transactions", 1, 0, 100)
	fake.offsetErrors["blnk.transactions"] = map[int]error{1: kafka.LeaderNotAvailable}

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{
		GroupID: "acme-recon-group",
		Topics:  []string{"blnk.transactions"},
	})
	require.NoError(t, err, "one unreadable partition must not void the whole measurement")

	require.Len(t, report.Topics, 1)
	assert.Equal(t, int64(10), report.TotalLag, "only the readable partition contributes")
	assert.Equal(t, 1, report.Topics[0].PartitionsUnavailable)

	require.Len(t, report.Topics[0].Partitions, 2)
	assert.False(t, report.Topics[0].Partitions[0].Unavailable)
	assert.True(t, report.Topics[0].Partitions[1].Unavailable,
		"the unreadable partition must be marked, not silently zeroed")
	assert.Zero(t, report.Topics[0].Partitions[1].Lag)
}

// TestConsumerLag_ReportsRequestedTopicsThatDoNotExist stops a mistyped or unprovisioned
// topic from reading as permanently healthy.
func TestConsumerLag_ReportsRequestedTopicsThatDoNotExist(t *testing.T) {
	fake := newFakeAdminClient()
	fake.withOffsets("blnk.transactions", 0, 0, 10).withCommitted("blnk.transactions", 0, 10)

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{
		GroupID: "acme-recon-group",
		Topics:  []string{"blnk.transactions", "blnk.typo"},
	})
	require.NoError(t, err)

	assert.Equal(t, []string{"blnk.typo"}, report.MissingTopics,
		"an absent topic must be named rather than silently contributing zero")
	assert.Len(t, report.Topics, 1, "only existing topics can be measured")
	assert.Zero(t, report.TotalLag)
}

// TestConsumerLag_RequiresAConsumerGroupAndToleratesAnEmptyGrant covers the two argument
// edge cases, which are deliberately treated differently.
//
// A missing group is a programming error: nothing can be measured without one. An empty
// topic list is a legitimate registry state — a subscriber authorised for nothing — and a
// metrics loop walking every subscriber must not be forced to special-case it.
func TestConsumerLag_RequiresAConsumerGroupAndToleratesAnEmptyGrant(t *testing.T) {
	gauge := captureConsumerLagGauge(t)
	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	_, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{Topics: []string{"blnk.transactions"}})
	require.Error(t, err, "a measurement without a consumer group is meaningless")
	assert.Contains(t, err.Error(), "consumer group")
	assert.Zero(t, fake.totalCalls())

	report, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{
		GroupID: "acme-recon-group",
		Topics:  []string{"", "   "},
	})
	require.NoError(t, err, "a subscriber authorised for nothing has no lag and no error")
	assert.Zero(t, report.TotalLag)
	assert.Empty(t, report.Topics)
	assert.Zero(t, fake.totalCalls(), "no request may be sent when there is nothing to measure")
	assert.Empty(t, gauge.snapshot(), "no gauge value may be published for a measurement that did not happen")
}

// TestConsumerLag_ReadsBothOffsetBoundsInOneRequest pins the request shape.
//
// Both bounds are needed — the end offset for the lag, the first offset as the baseline for
// an uncommitted partition — and asking for them in one ListOffsets call means they describe
// the same instant. Two calls would read them a round trip apart, which is how a lag figure
// acquires a systematic error.
func TestConsumerLag_ReadsBothOffsetBoundsInOneRequest(t *testing.T) {
	fake := newFakeAdminClient()
	fake.withOffsets("blnk.transactions", 0, 0, 10).withOffsets("blnk.transactions", 1, 0, 10)

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	_, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{
		GroupID: "acme-recon-group",
		Topics:  []string{"blnk.transactions"},
	})
	require.NoError(t, err)

	assert.Equal(t, 1, fake.callCount("ListOffsets"), "one request must cover every partition")
	assert.Equal(t, 1, fake.callCount("OffsetFetch"))

	fake.mu.Lock()
	request := fake.listOffsetsRequests[0]
	fake.mu.Unlock()

	requests := request.Topics["blnk.transactions"]
	require.Len(t, requests, 4, "two partitions, each asked for its first and last offset")

	timestamps := map[int64]int{}
	for _, offsetRequest := range requests {
		timestamps[offsetRequest.Timestamp]++
	}
	assert.Equal(t, 2, timestamps[kafka.FirstOffset], "each partition's first offset must be requested")
	assert.Equal(t, 2, timestamps[kafka.LastOffset], "each partition's end offset must be requested")
	assert.Equal(t, kafka.ReadUncommitted, request.IsolationLevel,
		"the conventional LOG-END-OFFSET definition is what operators compare against")
}

// TestTopicEndOffsets_DefaultsToTheWholeInventory pins the reconciliation's default scope.
//
// The daily check compares outbox counts against the summed offsets of every topic Blnk
// owns, so an unnamed call must cover exactly the eight-topic inventory — no more, and
// crucially no fewer, since a missing dead-letter topic would make the broker side look
// short and read as message loss.
func TestTopicEndOffsets_DefaultsToTheWholeInventory(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	fake := newFakeAdminClient()
	for _, topic := range expectedEventTopics {
		fake.withOffsets(topic, 0, 0, 1)
	}

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.TopicEndOffsets(context.Background())
	require.NoError(t, err)

	fake.mu.Lock()
	requested := fake.metadataRequests[0].Topics
	fake.mu.Unlock()

	assert.Equal(t, expectedEventTopics, requested,
		"an unnamed call must measure the whole inventory, category topics and dead-letter siblings alike")
	assert.Len(t, report.Topics, len(expectedEventTopics))
	assert.Equal(t, int64(len(expectedEventTopics)), report.EndOffsetSum)
	assert.Empty(t, report.MissingTopics)
	assert.Zero(t, report.PartitionsUnavailable,
		"the reconciliation is only valid when every partition was readable")
}

// TestTopicEndOffsets_SumsEndOffsetsAndSeparatesRetention keeps the two figures apart.
//
// The end-offset sum counts every record ever published and is the reconciliation figure.
// The retained count is what is still on the log, and it legitimately falls below the outbox
// count once retention deletes records — which is exactly why reporting only one number
// would make a retention-caused discrepancy indistinguishable from real loss.
func TestTopicEndOffsets_SumsEndOffsetsAndSeparatesRetention(t *testing.T) {
	fake := newFakeAdminClient()
	fake.withOffsets("blnk.transactions", 0, 0, 100)   // nothing deleted
	fake.withOffsets("blnk.transactions", 1, 40, 100)  // head trimmed
	fake.withOffsets("blnk.transactions.dlt", 0, 0, 3) // three dead letters

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.TopicEndOffsets(context.Background(), "blnk.transactions", "blnk.transactions.dlt")
	require.NoError(t, err)

	assert.Equal(t, int64(203), report.EndOffsetSum, "every record ever published across both topics")
	assert.Equal(t, int64(163), report.RetainedCount, "retention deleted the first forty of one partition")

	main, found := report.Lookup("blnk.transactions")
	require.True(t, found)
	assert.Equal(t, int64(200), main.EndOffsetSum)
	assert.Equal(t, int64(160), main.RetainedCount)
	require.Len(t, main.Partitions, 2)
	assert.Equal(t, int64(40), main.Partitions[1].FirstOffset)
	assert.Equal(t, int64(100), main.Partitions[1].EndOffset)

	assert.Equal(t, map[string]int64{"blnk.transactions": 200, "blnk.transactions.dlt": 3},
		report.EndOffsetsByTopic(),
		"the per-topic reduction is the shape the outbox statistics endpoint reports")
}

// TestTopicEndOffsets_FlagsWhatWouldInvalidateTheReconciliation covers the two caveats.
//
// A missing topic and an unreadable partition both shorten the broker side of the
// comparison. Reporting them is what lets the runbook say "reconcile only when both are
// clear" instead of an operator concluding that events were lost.
func TestTopicEndOffsets_FlagsWhatWouldInvalidateTheReconciliation(t *testing.T) {
	fake := newFakeAdminClient()
	fake.withOffsets("blnk.transactions", 0, 0, 10)
	fake.withOffsets("blnk.transactions", 1, 0, 10)
	fake.offsetErrors["blnk.transactions"] = map[int]error{1: kafka.LeaderNotAvailable}

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.TopicEndOffsets(context.Background(), "blnk.transactions", "blnk.absent")
	require.NoError(t, err)

	assert.Equal(t, []string{"blnk.absent"}, report.MissingTopics)
	assert.Equal(t, 1, report.PartitionsUnavailable)
	assert.Equal(t, int64(10), report.EndOffsetSum, "an unreadable partition contributes nothing to the sum")

	snapshot, found := report.Lookup("blnk.transactions")
	require.True(t, found)
	require.Len(t, snapshot.Partitions, 2)
	assert.True(t, snapshot.Partitions[1].Unavailable)
}

// TestRetainedRecords_NeverGoesNegative covers the retention arithmetic in isolation.
func TestRetainedRecords_NeverGoesNegative(t *testing.T) {
	cases := []struct {
		name     string
		first    int64
		end      int64
		expected int64
	}{
		{name: "untrimmed log", first: 0, end: 10, expected: 10},
		{name: "trimmed log", first: 4, end: 10, expected: 6},
		{name: "fully trimmed log", first: 10, end: 10, expected: 0},
		{name: "empty partition", first: 0, end: 0, expected: 0},
		{name: "unknown first offset", first: -1, end: 10, expected: 10},
		{name: "unreadable partition", first: -1, end: -1, expected: 0},
		{name: "first beyond end", first: 20, end: 10, expected: 0},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			retained := retainedRecords(testCase.first, testCase.end)

			assert.Equal(t, testCase.expected, retained)
			assert.GreaterOrEqual(t, retained, int64(0), "a retained count must never be negative")
		})
	}
}

// TestOffsetBoundsFor_TreatsAnAbsentReadingAsUnavailable pins the lookup that keeps a
// missing measurement from becoming a real zero.
//
// Indexing the nested maps directly would yield the zero value — end offset 0, available —
// which reads as a legitimately empty partition. In a lag measurement that hides a backlog;
// in the reconciliation it shortens the broker side and looks exactly like message loss.
func TestOffsetBoundsFor_TreatsAnAbsentReadingAsUnavailable(t *testing.T) {
	bounds := map[string]map[int]partitionOffsetBounds{
		"blnk.transactions": {
			0: {first: 0, end: 0, unavailable: false},
		},
	}

	present := offsetBoundsFor(bounds, "blnk.transactions", 0)
	assert.False(t, present.unavailable, "a partition the broker reported as empty is available with end offset 0")
	assert.Equal(t, int64(0), present.end)

	missingPartition := offsetBoundsFor(bounds, "blnk.transactions", 7)
	assert.True(t, missingPartition.unavailable, "an unreported partition must be unavailable, not empty")
	assert.Equal(t, int64(-1), missingPartition.end)

	missingTopic := offsetBoundsFor(bounds, "blnk.balances", 0)
	assert.True(t, missingTopic.unavailable, "an unreported topic must be unavailable, not empty")

	fromNil := offsetBoundsFor(nil, "blnk.balances", 0)
	assert.True(t, fromNil.unavailable, "a nil result set must not read as a cluster full of empty partitions")
}

// TestCommittedOffsetFor_ReportsTheBrokersOwnSentinel keeps one representation of "no
// commit" in play.
func TestCommittedOffsetFor_ReportsTheBrokersOwnSentinel(t *testing.T) {
	committed := map[string]map[int]int64{"blnk.transactions": {0: 42}}

	assert.Equal(t, int64(42), committedOffsetFor(committed, "blnk.transactions", 0))
	assert.Equal(t, int64(-1), committedOffsetFor(committed, "blnk.transactions", 1),
		"an uncommitted partition must read as the broker's own -1")
	assert.Equal(t, int64(-1), committedOffsetFor(committed, "blnk.balances", 0))
	assert.Equal(t, int64(-1), committedOffsetFor(nil, "blnk.transactions", 0))
}

// TestNewKafkaAdmin_EmptyBrokersYieldsAnUnconfiguredClientThatFailsFast is the graceful
// degradation contract.
//
// An empty KAFKA_BROKERS is a legitimate steady state: it is what lets every existing
// deployment and the whole existing test suite run unchanged. Construction must therefore
// succeed and perform no I/O, and every operation must then fail immediately with a typed
// error rather than dialling nothing until a timeout.
func TestNewKafkaAdmin_EmptyBrokersYieldsAnUnconfiguredClientThatFailsFast(t *testing.T) {
	for name, configuration := range map[string]*config.Configuration{
		"nil configuration": nil,
		"no brokers":        {Kafka: config.KafkaConfig{}},
		"blank brokers":     {Kafka: config.KafkaConfig{Brokers: []string{"", "   "}}},
	} {
		t.Run(name, func(t *testing.T) {
			admin, err := NewKafkaAdmin(configuration)
			require.NoError(t, err, "an absent broker list is not an error")
			require.NotNil(t, admin)

			assert.False(t, admin.IsConfigured())
			assert.Nil(t, admin.Brokers())
			assert.NoError(t, admin.Close(), "closing an unconfigured client must be safe")

			ctx := context.Background()

			_, err = admin.EnsureTopics(ctx)
			assert.ErrorIs(t, err, ErrKafkaAdminNotConfigured)

			_, err = admin.ProvisionSubscriberPrincipal(ctx,
				NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword))
			assert.ErrorIs(t, err, ErrKafkaAdminNotConfigured)

			_, err = admin.SubscriberCredentialExists(ctx, "acme-recon")
			assert.ErrorIs(t, err, ErrKafkaAdminNotConfigured)

			_, err = admin.AuthorizerActive(ctx)
			assert.ErrorIs(t, err, ErrKafkaAdminNotConfigured)

			_, err = admin.ConsumerLag(ctx, ConsumerLagRequest{GroupID: "g", Topics: []string{"t"}})
			assert.ErrorIs(t, err, ErrKafkaAdminNotConfigured)

			_, err = admin.TopicEndOffsets(ctx)
			assert.ErrorIs(t, err, ErrKafkaAdminNotConfigured)
		})
	}
}

// TestNewKafkaAdmin_BuildsAScramAuthenticatedTransport asserts the administrative
// authentication.
//
// SHA-512 is fixed rather than negotiated because it is the mechanism the bootstrap script
// seeds and the mechanism every subscriber credential is provisioned with; a different
// choice here could only ever be a mismatch that surfaces as an authentication failure.
func TestNewKafkaAdmin_BuildsAScramAuthenticatedTransport(t *testing.T) {
	admin, err := NewKafkaAdmin(&config.Configuration{
		Kafka: config.KafkaConfig{
			Brokers:           []string{" broker-1:9092 ", "", "broker-2:9092"},
			SASLAdminUser:     "admin",
			SASLAdminSecret:   "REDACTED_TEST_SECRET",
			MinPartitions:     2,
			ReplicationFactor: 1,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, admin)
	t.Cleanup(func() { assert.NoError(t, admin.Close()) })

	assert.True(t, admin.IsConfigured())
	assert.Equal(t, []string{"broker-1:9092", "broker-2:9092"}, admin.Brokers(),
		"blank broker entries must be dropped rather than dialled as \":9092\"")

	require.NotNil(t, admin.transport)
	require.NotNil(t, admin.transport.SASL, "a configured administrative user must produce SASL authentication")
	assert.Equal(t, SubscriberSASLMechanism, admin.transport.SASL.Name(),
		"administrative authentication must use the same mechanism subscribers are provisioned with")
	assert.Equal(t, kafkaAdminClientID, admin.transport.ClientID,
		"the client must identify itself so its traffic is distinguishable in broker logs")
	assert.Equal(t, kafkaAdminDialTimeout, admin.transport.DialTimeout,
		"dialling must be bounded well inside the five-second provisioning budget")

	assert.Equal(t, MinTopicPartitions, admin.partitions,
		"a configured partition count below the required minimum must be raised")
	assert.Equal(t, 1, admin.replicationFactor, "the replication factor must be carried through verbatim")

	brokers := admin.Brokers()
	brokers[0] = "mutated"
	assert.Equal(t, []string{"broker-1:9092", "broker-2:9092"}, admin.Brokers(),
		"Brokers must hand out a copy so a caller cannot rewrite what the client dials")
}

// TestNewKafkaAdmin_RejectsAnAdminUserWithoutASecret catches the one genuinely broken
// credential combination at construction, where it reads as a configuration problem, rather
// than at the first request, where it reads as a wrong password.
func TestNewKafkaAdmin_RejectsAnAdminUserWithoutASecret(t *testing.T) {
	admin, err := NewKafkaAdmin(&config.Configuration{
		Kafka: config.KafkaConfig{
			Brokers:           []string{"broker-1:9092"},
			SASLAdminUser:     "admin",
			ReplicationFactor: 1,
		},
	})
	require.Error(t, err)
	assert.Nil(t, admin)
	assert.Contains(t, err.Error(), "KAFKA_SASL_ADMIN_SECRET", "the error must name the missing variable")
}

// TestNewKafkaAdmin_AllowsAPlaintextBrokerWithoutSASL keeps a broker with no SASL listener
// usable: attaching a mechanism it does not offer would fail the handshake.
func TestNewKafkaAdmin_AllowsAPlaintextBrokerWithoutSASL(t *testing.T) {
	admin, err := NewKafkaAdmin(&config.Configuration{
		Kafka: config.KafkaConfig{
			Brokers:           []string{"broker-1:9092"},
			MinPartitions:     MinTopicPartitions,
			ReplicationFactor: 1,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, admin)
	t.Cleanup(func() { assert.NoError(t, admin.Close()) })

	assert.True(t, admin.IsConfigured())
	assert.Nil(t, admin.transport.SASL, "no administrative user means no SASL mechanism")
}

// TestKafkaAdminClient_EveryOperationHonoursACancelledContext proves cancellation is
// checked BEFORE any round trip.
//
// Without that check an already-expired context would still cost a network round trip
// before the client library noticed, which is precisely what a five-second provisioning
// budget cannot afford.
func TestKafkaAdminClient_EveryOperationHonoursACancelledContext(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	operations := map[string]func(ctx context.Context, admin *KafkaAdminClient) error{
		"EnsureTopics": func(ctx context.Context, admin *KafkaAdminClient) error {
			_, err := admin.EnsureTopics(ctx)

			return err
		},
		"ProvisionSubscriberPrincipal": func(ctx context.Context, admin *KafkaAdminClient) error {
			_, err := admin.ProvisionSubscriberPrincipal(ctx,
				NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword))

			return err
		},
		"SubscriberCredentialExists": func(ctx context.Context, admin *KafkaAdminClient) error {
			_, err := admin.SubscriberCredentialExists(ctx, "acme-recon")

			return err
		},
		"AuthorizerActive": func(ctx context.Context, admin *KafkaAdminClient) error {
			_, err := admin.AuthorizerActive(ctx)

			return err
		},
		"ConsumerLag": func(ctx context.Context, admin *KafkaAdminClient) error {
			_, err := admin.ConsumerLag(ctx, ConsumerLagRequest{
				GroupID: "acme-recon-group",
				Topics:  []string{"blnk.transactions"},
			})

			return err
		},
		"TopicEndOffsets": func(ctx context.Context, admin *KafkaAdminClient) error {
			_, err := admin.TopicEndOffsets(ctx)

			return err
		},
	}

	for name, operation := range operations {
		t.Run(name+" cancelled", func(t *testing.T) {
			fake := newFakeAdminClient()
			admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err := operation(ctx, admin)
			require.Error(t, err, "%s must refuse a cancelled context", name)
			assert.ErrorIs(t, err, context.Canceled, "the cancellation must stay inspectable through the wrapper")
			assert.Zero(t, fake.totalCalls(), "%s must not reach the broker with a cancelled context", name)
		})

		t.Run(name+" expired budget", func(t *testing.T) {
			fake := newFakeAdminClient()
			admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

			// A five-second budget that has already elapsed, which is what a caller under
			// the credential-issuance deadline hands over when an earlier step overran.
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
			defer cancel()

			err := operation(ctx, admin)
			require.Error(t, err, "%s must refuse an expired budget", name)
			assert.ErrorIs(t, err, context.DeadlineExceeded)
			assert.Zero(t, fake.totalCalls(), "%s must not reach the broker with an expired budget", name)
		})
	}
}

// TestProvisionSubscriberPrincipal_PropagatesTheFiveSecondBudgetToEveryRoundTrip proves the
// deadline is not merely respected at the entry point.
//
// Credential issuance must complete inside five seconds, and it makes up to four round
// trips. Each has to inherit the same deadline, otherwise a slow broker turns a bounded
// operation into an unbounded one.
func TestProvisionSubscriberPrincipal_PropagatesTheFiveSecondBudgetToEveryRoundTrip(t *testing.T) {
	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, err := admin.ProvisionSubscriberPrincipal(ctx,
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword))
	require.NoError(t, err, "provisioning must complete comfortably inside a five-second budget")
	assert.Equal(t, 5, result.ACLBindings)

	assert.True(t, fake.everyCallHadADeadline(),
		"every administrative round trip must inherit the caller's deadline")
	assert.GreaterOrEqual(t, fake.totalCalls(), 4,
		"provisioning describes, upserts, probes the authorizer and binds")
}

// TestKafkaAdminClient_CloseIsIdempotent covers the shared client being released from a
// process that may have more than one shutdown path.
func TestKafkaAdminClient_CloseIsIdempotent(t *testing.T) {
	admin, err := NewKafkaAdmin(&config.Configuration{
		Kafka: config.KafkaConfig{Brokers: []string{"broker-1:9092"}, ReplicationFactor: 1},
	})
	require.NoError(t, err)

	assert.NoError(t, admin.Close())
	assert.NoError(t, admin.Close(), "closing twice must be safe")

	var absent *KafkaAdminClient
	assert.NoError(t, absent.Close(), "closing a nil client must not panic")
	assert.False(t, absent.IsConfigured())
	assert.Nil(t, absent.Brokers())
}

// TestKafkaAdmin_InterfaceIsSatisfiedByTheConcreteClient keeps the published surface honest.
//
// The subscriber service and the events API depend on the interface, so a method dropped
// from the concrete client has to fail here rather than in whichever package happens to
// call it.
func TestKafkaAdmin_InterfaceIsSatisfiedByTheConcreteClient(t *testing.T) {
	var admin KafkaAdmin = newTestKafkaAdmin(newFakeAdminClient(), MinTopicPartitions, 1)

	require.NotNil(t, admin)
	assert.True(t, admin.IsConfigured())
	assert.NotEmpty(t, admin.Brokers())
	assert.NoError(t, admin.Close())
}

// TestRecordConsumerLag_ToleratesAnUninitialisedInstrument keeps a measurement path from
// taking a ledger process down in a build where the instruments were never created.
func TestRecordConsumerLag_ToleratesAnUninitialisedInstrument(t *testing.T) {
	original := metrics.SubscriberConsumerLag
	t.Cleanup(func() { metrics.SubscriberConsumerLag = original })

	metrics.SubscriberConsumerLag = nil

	assert.NotPanics(t, func() {
		recordConsumerLag(context.Background(), "sub", "group", "blnk.transactions", 7)
	})
}

// TestNormalizeTopicList_DropsBlanksAndDuplicatesInOrder covers the shared normalisation
// every report's ordering depends on.
func TestNormalizeTopicList_DropsBlanksAndDuplicatesInOrder(t *testing.T) {
	assert.Equal(t,
		[]string{"blnk.transactions", "blnk.balances"},
		normalizeTopicList([]string{" blnk.transactions", "", "  ", "blnk.balances", "blnk.transactions "}),
		"order must follow the caller, with blanks and duplicates removed")

	assert.Nil(t, normalizeTopicList(nil))
	assert.Nil(t, normalizeTopicList([]string{"", "   "}))
}

// TestMissingTopics_NamesTheAbsentOnesInRequestedOrder covers the absence reporting both
// measurements rely on.
func TestMissingTopics_NamesTheAbsentOnesInRequestedOrder(t *testing.T) {
	present := map[string][]int{"blnk.transactions": {0}, "blnk.balances": {0}}

	assert.Equal(t, []string{"blnk.identities", "blnk.system"},
		missingTopics([]string{"blnk.transactions", "blnk.identities", "blnk.balances", "blnk.system"}, present))
	assert.Nil(t, missingTopics([]string{"blnk.transactions"}, present))
	assert.Equal(t, []string{"blnk.transactions"}, missingTopics([]string{"blnk.transactions"}, nil))
}

// TestEventAdminSource_NeverHandsThePasswordToALogOrAnErrorCall is a structural guarantee,
// asserted over the syntax tree rather than over one execution.
//
// The runtime tests above prove the password does not leak on the paths they exercise. This
// proves it cannot leak on ANY path, including one added later, by checking that no logging
// or error-formatting call anywhere in the file takes the password as an argument.
func TestEventAdminSource_NeverHandsThePasswordToALogOrAnErrorCall(t *testing.T) {
	parsed, fileSet := parseEventAdminSource(t)

	// Names that hold or produce the secret. SaltedPassword is deliberately absent: it is
	// a one-way derivation and logging it, while pointless, would not disclose the secret.
	secretNames := map[string]struct{}{
		"Password": {},
		"password": {},
	}

	loggingOrFormatting := func(call *ast.CallExpr) bool {
		var name string
		switch function := call.Fun.(type) {
		case *ast.SelectorExpr:
			name = function.Sel.Name
		case *ast.Ident:
			name = function.Name
		default:
			return false
		}

		switch name {
		case "Errorf", "New", "Sprintf", "Sprint", "Print", "Printf",
			"Debug", "Debugf", "Info", "Infof", "Warn", "Warnf", "Error", "Errorf2",
			"Fatal", "Fatalf", "Panic", "Panicf",
			"WithField", "WithFields", "WithError", "String", "Int", "Int64", "Bool":
			return true
		default:
			return false
		}
	}

	referencesSecret := func(node ast.Node) bool {
		found := false
		ast.Inspect(node, func(inner ast.Node) bool {
			switch value := inner.(type) {
			case *ast.Ident:
				if _, secret := secretNames[value.Name]; secret {
					found = true
				}
			case *ast.SelectorExpr:
				if _, secret := secretNames[value.Sel.Name]; secret {
					found = true
				}
			}

			return !found
		})

		return found
	}

	ast.Inspect(parsed, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || !loggingOrFormatting(call) {
			return true
		}

		for _, argument := range call.Args {
			if referencesSecret(argument) {
				t.Errorf(
					"%s: a logging or error-formatting call takes the SCRAM password as an argument; "+
						"the secret must never reach a log line or an error message",
					fileSet.Position(argument.Pos()),
				)
			}
		}

		return true
	})
}

// TestEventAdminSource_SpellsNoTopicNameAsALiteral enforces the single source of truth for
// topic naming.
//
// A literal here would ignore KAFKA_TOPIC_PREFIX and provision, or measure, topics that
// nothing publishes to — a failure with no error and no log line, visible only when a
// subscriber eventually notices missing events.
func TestEventAdminSource_SpellsNoTopicNameAsALiteral(t *testing.T) {
	parsed, fileSet := parseEventAdminSource(t)

	ast.Inspect(parsed, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}

		value := strings.Trim(literal.Value, "`\"")
		if strings.HasPrefix(value, DefaultTopicPrefix+".") || strings.HasSuffix(value, DeadLetterTopicSuffix) {
			t.Errorf(
				"%s: %s looks like a topic name spelled as a literal; every name must come from "+
					"event_topics.go so that KAFKA_TOPIC_PREFIX is honoured",
				fileSet.Position(literal.Pos()), literal.Value,
			)
		}

		return true
	})
}

// TestEventAdminSource_StaysWithinItsMandate pins the boundaries the plan draws around this
// file, each of which is a real hazard rather than a style preference.
func TestEventAdminSource_StaysWithinItsMandate(t *testing.T) {
	parsed, _ := parseEventAdminSource(t)

	imports := map[string]struct{}{}
	for _, spec := range parsed.Imports {
		imports[strings.Trim(spec.Path.Value, "\"")] = struct{}{}
	}

	forbiddenImports := map[string]string{
		"os/exec":                              "provisioning is native Go; shelling out to kafka-configs.sh or kafka-acls.sh would need a JVM on every image and would lose typed error codes",
		"database/sql":                         "subscriber persistence belongs to database/event_subscriber.go",
		"github.com/blnkfinance/blnk/database": "this file talks to a broker, never to a database",
	}
	for path, reason := range forbiddenImports {
		_, present := imports[path]
		assert.False(t, present, "event_admin.go must not import %s: %s", path, reason)
	}

	assert.Contains(t, imports, "github.com/segmentio/kafka-go")
	assert.Contains(t, imports, "github.com/segmentio/kafka-go/sasl/scram")
	assert.Contains(t, imports, "github.com/blnkfinance/blnk/internal/metrics",
		"the consumer-lag figure must feed the shared gauge")

	// The forbidden names are looked for in the SYNTAX TREE rather than in the file text,
	// because the file discusses several of them in its comments — explaining why they are
	// deliberately not wrapped is part of the documentation, and a text search cannot tell
	// an explanation apart from a call.
	referenced := map[string]struct{}{}
	ast.Inspect(parsed, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.SelectorExpr:
			referenced[value.Sel.Name] = struct{}{}
		case *ast.Ident:
			referenced[value.Name] = struct{}{}
		}

		return true
	})

	forbiddenReferences := map[string]string{
		"DeleteTopics":  "no normal operation may destroy an event topic; it would discard undelivered events for every subscriber",
		"DeleteACLs":    "revoking a boundary is not part of provisioning",
		"WriteMessages": "producing belongs to the publisher, and only the relay may write events",
		"Writer":        "an admin client that could also produce would make it possible to bypass the outbox",
		"Reader":        "consuming, consumer error handling and subscriber-side dead-lettering are out of scope",
		"Command":       "provisioning is native Go; shelling out would need a JVM on every image",
	}
	for name, reason := range forbiddenReferences {
		_, present := referenced[name]
		assert.False(t, present, "event_admin.go must not reference %s: %s", name, reason)
	}

	assert.Contains(t, referenced, "SubscriberConsumerLag",
		"the lag figure must be published on the instrument the alert rule reads")
	_, declaresOwnMeter := referenced["Meter"]
	assert.False(t, declaresOwnMeter,
		"a second instrument for the same measurement would produce a series the alert rule does not read")

	assert.Contains(t, readEventAdminSource(t),
		"authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
		"the ACL-enforcement caveat must be stated in the source, because without the authorizer the "+
			"isolation criterion passes vacuously")
}

// readEventAdminSource returns event_admin.go as text, for assertions that are about the
// presence or absence of a reference rather than about syntax.
func readEventAdminSource(t *testing.T) string {
	t.Helper()

	path := filepath.Join(moduleRootDir(t), "event_admin.go")
	contents, err := os.ReadFile(path)
	require.NoError(t, err, "event_admin.go must be readable")

	return string(contents)
}
