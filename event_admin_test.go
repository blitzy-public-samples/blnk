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
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
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

	apimodel "github.com/blnkfinance/blnk/api/model"
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
// The ten topic names, the six-partition floor, the 4096-iteration minimum and the
// three gauge attribute keys are all written out LONGHAND below rather than derived from
// the code under test. A test that asks the implementation what it expects agrees with
// any implementation, including a broken one.
//
// The longhand inventory is then tied back to event_topics.go — the one place topic names
// are composed — by TestEventTopicInventory_MatchesTheSingleSourceOfTruth. That is the
// bridge that lets both properties hold at once: the literal list keeps every assertion
// here non-vacuous, and the bridge stops the literal list from drifting away from the
// names the pipeline actually publishes to.

// expectedEventTopics is the topic inventory, spelled out independently of
// event_topics.go: the five category topics followed by their five dead-letter siblings,
// in the canonical order provisioning uses.
//
// blnk.quarantine is where an event type the catalogue does not recognise is routed. It is
// INTERNAL — no subscriber can be granted it — but Blnk itself writes to it, so it must be
// provisioned with the same geometry as every other topic. A quarantine topic that does not
// exist would strand exactly the events that already indicate a routing defect.
var expectedEventTopics = []string{
	"blnk.transactions",
	"blnk.balances",
	"blnk.identities",
	"blnk.system",
	"blnk.quarantine",
	"blnk.transactions.dlt",
	"blnk.balances.dlt",
	"blnk.identities.dlt",
	"blnk.system.dlt",
	"blnk.quarantine.dlt",
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
	// replicas maps a topic to the size of each partition's replica set. An absent entry
	// models a broker that reported no replica information, which the assurance pass treats
	// as "not visible yet" rather than as under-replication.
	replicas map[string]int
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
	// deleteACLErrors is returned positionally as the per-filter result of DeleteACLs.
	deleteACLErrors []error
	// scramDeleteError is returned as the per-user result of a credential deletion.
	scramDeleteError error

	// --- Recordings ---

	calls                    []string
	deadlines                []bool
	createTopicsRequests     []*kafka.CreateTopicsRequest
	createPartitionsRequests []*kafka.CreatePartitionsRequest
	createACLsRequests       []*kafka.CreateACLsRequest
	deleteACLsRequests       []*kafka.DeleteACLsRequest
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
		replicas:            map[string]int{},
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

// withReplicas sets the replica-set size the topic's partitions report, creating the topic
// with the given partition count if it does not exist.
//
// It models the ONE fact the assurance pass could previously not see: a topic's actual
// durability, as opposed to the factor it was requested with.
func (f *fakeAdminClient) withReplicas(topic string, partitions, replicas int) *fakeAdminClient {
	f.withTopic(topic, partitions)
	f.replicas[topic] = replicas

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

		replicaCount := f.replicas[name]
		for _, id := range ids {
			partition := kafka.Partition{Topic: name, ID: id}
			for broker := 0; broker < replicaCount; broker++ {
				partition.Replicas = append(partition.Replicas, kafka.Broker{ID: broker})
			}
			topic.Partitions = append(topic.Partitions, partition)
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

// DeleteACLs models binding removal.
//
// It reports the MATCHING bindings back, as the broker does, so a test can assert that
// revocation removed exactly the bindings provisioning created rather than merely that a
// request was sent. Deleting a binding that was never created matches nothing and is not an
// error, which is the idempotence the compensation path depends on.
func (f *fakeAdminClient) DeleteACLs(
	ctx context.Context,
	req *kafka.DeleteACLsRequest,
) (*kafka.DeleteACLsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.record("DeleteACLs", ctx); err != nil {
		return nil, err
	}
	f.deleteACLsRequests = append(f.deleteACLsRequests, req)

	response := &kafka.DeleteACLsResponse{Results: make([]kafka.DeleteACLsResult, 0, len(req.Filters))}
	for i, filter := range req.Filters {
		result := kafka.DeleteACLsResult{}
		if i < len(f.deleteACLErrors) {
			result.Error = f.deleteACLErrors[i]
		}

		if result.Error == nil {
			result.MatchingACLs = append(result.MatchingACLs, kafka.DeleteACLsMatchingACLs{
				ResourceType:        filter.ResourceTypeFilter,
				ResourceName:        filter.ResourceNameFilter,
				ResourcePatternType: filter.ResourcePatternTypeFilter,
				Principal:           filter.PrincipalFilter,
				Host:                filter.HostFilter,
				Operation:           filter.Operation,
				PermissionType:      filter.PermissionType,
			})
		}

		response.Results = append(response.Results, result)
	}

	return response, nil
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

	// Deletions are modelled on the same request, as the protocol does. A deletion for a
	// principal that holds no credential answers RESOURCE_NOT_FOUND, exactly as the broker
	// does, so the idempotence revocation relies on is exercised rather than assumed.
	for _, deletion := range req.Deletions {
		resultErr := f.scramDeleteError
		if resultErr == nil {
			if _, exists := f.scram[deletion.Name]; !exists {
				resultErr = kafka.ResourceNotFound
			} else {
				delete(f.scram, deletion.Name)
			}
		}

		response.Results = append(response.Results, kafka.AlterUserScramCredentialsResponseUser{
			User:  deletion.Name,
			Error: resultErr,
		})
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

// TestEventTopicInventory_MatchesTheSingleSourceOfTruth ties the longhand inventory above
// to event_topics.go, which is the one place topic names are composed.
//
// It is the bridge that lets the literal list and the derived list coexist without either
// being redundant. The literal list is what stops every other assertion in this file from
// being vacuous — a test that asks the implementation what it expects agrees with any
// implementation, including one that provisions nothing. THIS test is what stops the literal
// list from silently drifting: if the category set, the prefix resolution or the dead-letter
// suffix ever changes, the two disagree here rather than in production, where the only
// symptom is a subscriber that never receives an event.
//
// The prefix is pinned to the default for the duration, because the literal names above are
// written with it.
func TestEventTopicInventory_MatchesTheSingleSourceOfTruth(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	assert.Equal(t, expectedEventTopics, AllTopicsWithDeadLetters(),
		"the inventory this file asserts against must be exactly the inventory event_topics.go composes, "+
			"so the test and the implementation share one source of truth")

	const categoryCount = 5

	require.Len(t, expectedEventTopics, categoryCount*2,
		"five category topics and one dead-letter sibling each")
	assert.Equal(t, expectedEventTopics[:categoryCount], AllTopics(),
		"the first five entries are the category topics, in canonical provisioning order")
	assert.Equal(t, expectedEventTopics[categoryCount:], AllDeadLetterTopics(),
		"the last five entries are their dead-letter siblings, in the same order")

	categories := EventCategories()
	require.Len(t, categories, categoryCount,
		"five categories are what give every emitted event type a home — including the quarantine category an unrecognised type routes to; a sixth would need a topic here")

	// The internal categories must be provisioned but not grantable. Both halves matter:
	// Blnk writes to them, so they need topics, and their contents are not subscriber
	// data, so no ACL may cover them.
	for _, category := range []string{model.EventCategorySystem, model.EventCategoryQuarantine} {
		topic := TopicForCategory(category)
		assert.Contains(t, expectedEventTopics, topic,
			"internal topic %q must still be provisioned: Blnk publishes to it", topic)
		assert.False(t, IsSubscriberGrantableTopic(topic),
			"internal topic %q must never be grantable to a subscriber", topic)
	}

	for index, category := range categories {
		topic := TopicForCategory(category)

		assert.Equal(t, expectedEventTopics[index], topic,
			"category %q must compose topic %q", category, expectedEventTopics[index])
		assert.Equal(t, expectedEventTopics[index+categoryCount], DLTFor(topic),
			"the dead-letter sibling of %q must follow the published <topic>.dlt convention", topic)
		assert.True(t, IsDeadLetterTopic(DLTFor(topic)),
			"a dead-letter topic must be recognisable as one, since the relay routes on that")
		assert.False(t, IsDeadLetterTopic(topic),
			"a category topic must never be mistaken for a dead-letter topic")
	}
}

// TestEnsureTopics_CreatesTheWholeInventoryWithTheConfiguredGeometry pins the inventory
// and the geometry of a first run against an empty broker.
//
// Every name is asserted exactly and in order. Topic naming has no runtime failure
// mode — a wrong name is a valid topic nobody reads — so an assertion that merely counted
// creations, or checked that the names were non-empty, would pass while the whole
// pipeline published into the void.
func TestEnsureTopics_CreatesTheWholeInventoryWithTheConfiguredGeometry(t *testing.T) {
	storeKafkaTopicPrefix(t, "")

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 3)

	report, err := admin.EnsureTopics(context.Background())
	require.NoError(t, err, "assuring topics against an empty broker must succeed")

	configs := createdTopicConfigs(fake)
	require.Len(t, configs, len(expectedEventTopics),
		"exactly the owned topics must be created: every category and its dead-letter sibling")

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

// TestEnsureTopics_TakesItsGeometryFromConfiguration walks the whole path a deployment
// actually takes: config.Kafka -> NewKafkaAdmin -> the outgoing CreateTopics request.
//
// The two tests above pin the geometry on a client whose fields were set directly, and the
// constructor tests further down pin the fields. Neither proves the JOIN, and the join is
// exactly where a hard-coded literal would hide: a constructor that read configuration
// faithfully and a creation path that ignored it would satisfy both halves separately while
// provisioning the wrong geometry. Injecting the fake seam into a CONFIGURATION-BUILT client
// is the only way to show the configured numbers arriving in the request.
//
// The two dimensions are asserted differently, and deliberately so:
//
//   - The partition count is a FLOOR. Six partitions is requirement R-6, so a configured
//     value below it is raised rather than honoured, and a value above it is honoured rather
//     than clamped.
//   - The replication factor is EXACT. A single-broker KRaft cluster rejects a factor of 3
//     outright with INVALID_REPLICATION_FACTOR, so a hard-coded 3 makes local bring-up
//     impossible; a hard-coded 1 would silently discard the durability requirement in
//     production. Both configured values are therefore asserted to arrive unchanged.
func TestEnsureTopics_TakesItsGeometryFromConfiguration(t *testing.T) {
	cases := []struct {
		name               string
		minPartitions      int
		replicationFactor  int
		expectedPartitions int
	}{
		{
			name:          "below the floor is raised",
			minPartitions: 1, replicationFactor: 1, expectedPartitions: MinTopicPartitions,
		},
		{
			name:          "unset is raised",
			minPartitions: 0, replicationFactor: 1, expectedPartitions: MinTopicPartitions,
		},
		{
			name:          "exactly the floor on a single-broker stack",
			minPartitions: MinTopicPartitions, replicationFactor: 1, expectedPartitions: MinTopicPartitions,
		},
		{
			name:          "production geometry",
			minPartitions: MinTopicPartitions, replicationFactor: 3, expectedPartitions: MinTopicPartitions,
		},
		{
			name:          "above the floor is honoured",
			minPartitions: 18, replicationFactor: 3, expectedPartitions: 18,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			storeKafkaTopicPrefix(t, "")

			admin, err := NewKafkaAdmin(&config.Configuration{
				Kafka: config.KafkaConfig{
					Brokers:           []string{"broker-1:9092"},
					MinPartitions:     testCase.minPartitions,
					InsecureLocalDev:  true, // the transport refuses plaintext without this
					ReplicationFactor: testCase.replicationFactor,
				},
			})
			require.NoError(t, err, "a configured broker list with a usable geometry must construct")
			t.Cleanup(func() { assert.NoError(t, admin.Close()) })

			// Substituting only the transport seam keeps everything else — the partition
			// count and the replication factor — exactly as configuration produced it, which
			// is what makes this an assertion about the join rather than about either half.
			fake := newFakeAdminClient()
			admin.client = fake

			report, err := admin.EnsureTopics(context.Background())
			require.NoError(t, err)

			configs := createdTopicConfigs(fake)
			require.Len(t, configs, len(expectedEventTopics),
				"the whole inventory must be provisioned regardless of geometry")

			for _, topicConfig := range configs {
				assert.Equal(t, testCase.expectedPartitions, topicConfig.NumPartitions,
					"topic %q must carry the partition count configuration resolves to", topicConfig.Topic)
				assert.GreaterOrEqual(t, topicConfig.NumPartitions, MinTopicPartitions,
					"topic %q must never be provisioned below the required minimum of six partitions",
					topicConfig.Topic)
				assert.Equal(t, testCase.replicationFactor, topicConfig.ReplicationFactor,
					"topic %q must carry the configured replication factor verbatim, never a literal",
					topicConfig.Topic)
			}

			assert.Equal(t, testCase.expectedPartitions, admin.partitions,
				"the client must resolve the configured partition count once, at construction")
			assert.Equal(t, testCase.replicationFactor, admin.replicationFactor,
				"the configured replication factor must be carried through without adjustment")
			assert.Equal(t, testCase.expectedPartitions, report.Partitions)
			assert.Equal(t, testCase.replicationFactor, report.ReplicationFactor)
		})
	}
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

	// The same operation once more, but with the broker itself answering
	// TOPIC_ALREADY_EXISTS for every topic rather than the metadata probe reporting them
	// present. That is what a second server instance starting at the same moment sees, and a
	// whole inventory answering "already exists" must still be a successful, non-creating run
	// — otherwise a rolling restart of two replicas fails one of them every time.
	concurrent := newFakeAdminClient()
	for _, topic := range expectedEventTopics {
		concurrent.createTopicErrors[topic] = kafka.TopicAlreadyExists
	}

	third, err := newTestKafkaAdmin(concurrent, MinTopicPartitions, 1).EnsureTopics(context.Background())
	require.NoError(t, err,
		"an inventory that answers \"already exists\" throughout must not fail the run")
	assert.Zero(t, third.CreatedCount, "nothing was created by this run")
	assert.Equal(t, len(expectedEventTopics), third.UnchangedCount,
		"every topic must be reported as unchanged once its real partition count is known")
	assert.Zero(t, concurrent.callCount("CreatePartitions"),
		"the winning provisioner used the configured geometry, so there is nothing to grow")
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

	hook := logtest.NewGlobal()
	defer hook.Reset()

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

	// "Detected and reported clearly rather than attempted" is two obligations. The refusal is
	// only useful if an operator can act on it, so the warning has to name the topic, the two
	// partition counts and the variable to change — a silent refusal would leave a topic
	// permanently disagreeing with configuration and nobody knowing why.
	var reported bool
	for _, entry := range hook.AllEntries() {
		if entry.Level != logrus.WarnLevel {
			continue
		}
		if entry.Data["topic"] != oversized {
			continue
		}

		reported = true

		assert.Equal(t, 12, entry.Data["partitions"], "the warning must state the topic's actual geometry")
		assert.Equal(t, MinTopicPartitions, entry.Data["configured"],
			"the warning must state the configured geometry it is refusing to impose")
		assert.Contains(t, entry.Message, "KAFKA_MIN_PARTITIONS",
			"the warning must name the variable an operator can change")
		assert.Contains(t, entry.Message, "reducing partitions",
			"the warning must say why the refusal is not a defect")
	}
	assert.True(t, reported,
		"an over-partitioned topic must be reported at warning level, not merely recorded in the report")
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

// TestResolveTopicPartitions_ReportsEveryCorrectionItMakes is the "never silently" half of
// the floor.
//
// Raising a configured value is correct, but doing it without a word means the number an
// operator stated and the number in effect differ with nothing anywhere to show it — and a
// status endpoint would then report the configured value back as though it had been
// honoured. A NEGATIVE value used to slip through the report for exactly this reason: the
// guard tested for a POSITIVE below-floor value, so the one input that cannot possibly have
// been intended was the one input corrected in silence.
//
// Zero stays silent, and must: zero means unset, the configuration defaults it, and nothing
// was overridden.
func TestResolveTopicPartitions_ReportsEveryCorrectionItMakes(t *testing.T) {
	const correctionWarning = "KAFKA_MIN_PARTITIONS is below the required minimum"

	cases := []struct {
		name       string
		configured int
		warns      bool
	}{
		{name: "unset is silent because nothing was stated", configured: 0, warns: false},
		{name: "a positive below-floor value is reported", configured: 2, warns: true},
		{name: "a negative value is reported", configured: -4, warns: true},
		{name: "exactly the floor needs no correction", configured: MinTopicPartitions, warns: false},
		{name: "above the floor is honoured", configured: 24, warns: false},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			hook := logtest.NewGlobal()
			defer hook.Reset()

			resolveTopicPartitions(testCase.configured)

			warned := false
			for _, entry := range hook.AllEntries() {
				if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, correctionWarning) {
					warned = true

					break
				}
			}

			assert.Equal(t, testCase.warns, warned,
				"a correction must be reported and a non-correction must stay quiet")
		})
	}
}

// sentinelPassword is a value that could not occur by accident, so any appearance of it in
// a log line, an error or a serialised result is proof that the secret escaped.
//
// It is a fake credential, not a real one. It is printable ASCII so it passes the derivation's
// own input rule, and it is long and varied enough to satisfy the PASS-01 strength floor —
// which a fixture must do honestly rather than by being exempted, since the floor is exactly
// what the provisioning boundary now enforces.
const sentinelPassword = "Sentinel-Do-Not-Log-9f3c1a7e-Kq4Zv8Rm2Tb6"

// testSubscriberID is the fixture's business key, and every identity in the fixture is
// DERIVED from it.
//
// It is spelled out as a constant so the derivations below cannot drift from the identifier
// they are supposed to come from, and so a reader can see that no name in the fixture was
// chosen by hand — which is the property SEC-03 turns into a rule.
const testSubscriberID = "sub_0f6e2c8a"

// testSubscriber returns a registry row with a realistic access boundary.
//
// The principal and consumer group are derived rather than written out, because provisioning
// refuses any other value: they ARE the access boundary, so accepting a caller's choice of
// them is accepting a caller's choice of boundary. A fixture that hard-coded "acme-recon"
// would be asserting a shape the production path no longer permits.
//
// PartitionKeyPrefix is set to prove it is carried and NOT enforced: it is an advisory
// client-side filter hint, and no ACL derives from it.
func testSubscriber() *model.EventSubscriber {
	prefix := "acme-"

	principal, err := model.CanonicalKafkaPrincipal(testSubscriberID)
	if err != nil {
		panic("test fixture: the subscriber identifier must derive a principal: " + err.Error())
	}

	group, err := model.CanonicalConsumerGroupID(testSubscriberID)
	if err != nil {
		panic("test fixture: the subscriber identifier must derive a consumer group: " + err.Error())
	}

	return &model.EventSubscriber{
		SubscriberID:       testSubscriberID,
		Name:               "Acme Reconciliation",
		KafkaPrincipal:     principal,
		ConsumerGroupID:    group,
		AuthorizedTopics:   []string{"blnk.transactions", "blnk.balances"},
		PartitionKeyPrefix: &prefix,
	}
}

// testSubscriberPrincipal is the ACL principal string the fixture's bindings carry.
func testSubscriberPrincipal(t *testing.T) string {
	t.Helper()

	principal, err := model.CanonicalKafkaPrincipal(testSubscriberID)
	require.NoError(t, err)

	return kafkaPrincipalPrefix + principal
}

// testSubscriberGroupNamespace is the PREFIXED ACL resource name the fixture's group binding
// carries. It is the namespace, not the group: the binding reserves everything under it.
func testSubscriberGroupNamespace(t *testing.T) string {
	t.Helper()

	namespace, err := model.CanonicalConsumerGroupNamespace(testSubscriberID)
	require.NoError(t, err)

	return namespace
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
	expectedPrincipal, err := model.CanonicalKafkaPrincipal(testSubscriberID)
	require.NoError(t, err)
	assert.Equal(t, expectedPrincipal, upsertion.Name,
		"the credential must belong to the principal DERIVED from the subscriber id")
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
	assert.Equal(t, strings.TrimPrefix(testSubscriberPrincipal(t), kafkaPrincipalPrefix), result.Principal)
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

	principal := testSubscriberPrincipal(t)

	expected := []kafka.ACLEntry{
		{
			ResourceType:        kafka.ResourceTypeTopic,
			ResourceName:        "blnk.transactions",
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           principal,
			Host:                ACLHostAny,
			Operation:           kafka.ACLOperationTypeRead,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		},
		{
			ResourceType:        kafka.ResourceTypeTopic,
			ResourceName:        "blnk.transactions",
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           principal,
			Host:                ACLHostAny,
			Operation:           kafka.ACLOperationTypeDescribe,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		},
		{
			ResourceType:        kafka.ResourceTypeTopic,
			ResourceName:        "blnk.balances",
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           principal,
			Host:                ACLHostAny,
			Operation:           kafka.ACLOperationTypeRead,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		},
		{
			ResourceType:        kafka.ResourceTypeTopic,
			ResourceName:        "blnk.balances",
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           principal,
			Host:                ACLHostAny,
			Operation:           kafka.ACLOperationTypeDescribe,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		},
		{
			ResourceType:        kafka.ResourceTypeGroup,
			ResourceName:        testSubscriberGroupNamespace(t),
			ResourcePatternType: kafka.PatternTypePrefixed,
			Principal:           principal,
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
	// The widest LEGITIMATE grant, which is the grantable allowlist rather than the whole
	// inventory: the dead-letter and internal topics are no longer grantable at all, so asking
	// for them is refused before any binding is built (see
	// TestProvisionSubscriberPrincipal_RefusesATopicOutsideTheGrantableAllowlist).
	subscriber.AuthorizedTopics = SubscriberGrantableTopics()

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

// TestProvisionSubscriberPrincipal_NeverReachesAnotherSubscribersTopicsOrGroup is the
// cross-subscriber half of the isolation criterion.
//
// The access model has NO per-tenant topics: every subscriber reads the same four category
// topics and their dead-letter siblings, so the only thing keeping one subscriber out of
// another's data is this ACL grant. That means the boundary has to be asserted from the
// outside in — not "the grant contains what it should", which the equality test above already
// pins, but "the grant contains nothing else at all", enumerated against the FULL inventory
// and against a second subscriber's namespace.
//
// The two grants are provisioned against separate fakes and then compared, because the
// failure this guards against is not one malformed binding but a shared boundary: a request
// assembled from two different registry rows, or a topic list that leaked between them.
//
// ⚠️ This asserts CONSTRUCTION, not ENFORCEMENT. In KRaft mode a broker enforces these
// bindings only when it is started with
// authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer. Without it
// CreateACLs succeeds, the bindings are visible in kafka-acls output, and every request from
// every principal is allowed — so an isolation test run against such a broker passes while
// proving nothing at all. Broker-side proof therefore belongs to
// event_isolation_integration_test.go, and the broker's authorizer configuration is part of
// that criterion rather than an environmental detail.
func TestProvisionSubscriberPrincipal_NeverReachesAnotherSubscribersTopicsOrGroup(t *testing.T) {
	// Two subscribers with deliberately disjoint boundaries. Both identities are DERIVED from
	// their subscriber ids, because that is the only form provisioning accepts — and it is
	// also what makes their group namespaces provably disjoint rather than disjoint by the
	// author's choice of names.
	acme := testSubscriber()
	acme.AuthorizedTopics = []string{"blnk.transactions"}

	globex := derivedSubscriber(t, "sub_9d3b1f04", "Globex Risk",
		[]string{"blnk.identities", "blnk.balances"})

	acmeTopics, acmeGroups := provisionAndCollectGrant(t, acme)
	globexTopics, globexGroups := provisionAndCollectGrant(t, globex)

	assert.Equal(t, map[string]struct{}{"blnk.transactions": {}}, acmeTopics,
		"only the single authorised topic may be bound for this subscriber")
	assert.Equal(t, map[string]struct{}{"blnk.identities": {}, "blnk.balances": {}}, globexTopics,
		"the other subscriber's grant must be exactly its own two topics")

	// Enumerated against the WHOLE inventory rather than against the other subscriber's list
	// alone, because the dead-letter siblings are what an accidental widening would most
	// plausibly reach: each one has its category topic's name as a prefix, so a literal
	// pattern turned prefixed would swallow it.
	for _, topic := range expectedEventTopics {
		if topic != "blnk.transactions" {
			assert.NotContains(t, acmeTopics, topic,
				"a subscriber authorised only for blnk.transactions must not be granted %q", topic)
		}
		if topic != "blnk.identities" && topic != "blnk.balances" {
			assert.NotContains(t, globexTopics, topic,
				"a subscriber authorised only for its two topics must not be granted %q", topic)
		}
	}

	for topic := range globexTopics {
		assert.NotContains(t, acmeTopics, topic,
			"one subscriber's grant must never include another subscriber's topic %q", topic)
	}

	// The reserved resource is the derived NAMESPACE, not the group the subscriber joins: the
	// binding is prefixed, so it covers every leaf the subscriber creates under it.
	acmeNamespace, err := model.CanonicalConsumerGroupNamespace(acme.SubscriberID)
	require.NoError(t, err)
	globexNamespace, err := model.CanonicalConsumerGroupNamespace(globex.SubscriberID)
	require.NoError(t, err)

	assert.Equal(t, map[string]struct{}{acmeNamespace: {}}, acmeGroups,
		"exactly one consumer-group namespace may be reserved, and it must be the subscriber's own")
	assert.Equal(t, map[string]struct{}{globexNamespace: {}}, globexGroups,
		"the other subscriber's reservation must likewise be its own and nothing more")

	// The group binding uses a PREFIXED pattern, which reserves "<group>*". That is the one
	// intentional widening in the model, and it must widen only inside the subscriber's own
	// namespace: a reserved prefix that is also a prefix of somebody else's group would hand
	// over their offsets and their coordinator.
	for reserved := range acmeGroups {
		assert.False(t, strings.HasPrefix(globex.ConsumerGroupID, reserved),
			"reserved prefix %q must not cover another subscriber's group %q",
			reserved, globex.ConsumerGroupID)
	}
	for reserved := range globexGroups {
		assert.False(t, strings.HasPrefix(acme.ConsumerGroupID, reserved),
			"reserved prefix %q must not cover another subscriber's group %q",
			reserved, acme.ConsumerGroupID)
	}
}

// derivedSubscriber builds a registry row whose Kafka identity is DERIVED from the identifier,
// which is the only form provisioning accepts.
//
// It exists so a test needing a second subscriber cannot accidentally hand-pick a principal or
// a group — the two values that ARE the access boundary.
func derivedSubscriber(t *testing.T, subscriberID, name string, topics []string) *model.EventSubscriber {
	t.Helper()

	principal, err := model.CanonicalKafkaPrincipal(subscriberID)
	require.NoError(t, err)
	group, err := model.CanonicalConsumerGroupID(subscriberID)
	require.NoError(t, err)

	return &model.EventSubscriber{
		SubscriberID:     subscriberID,
		Name:             name,
		KafkaPrincipal:   principal,
		ConsumerGroupID:  group,
		AuthorizedTopics: topics,
	}
}

// provisionAndCollectGrant provisions one subscriber against a fresh fake and returns the
// topic names and consumer-group namespaces its bindings actually covered.
//
// Every binding's principal is checked here rather than in the caller, so that a grant
// assembled from two different registry rows — one subscriber's principal paired with
// another's topics — cannot slip through as a set that merely looks right.
//
// Returns:
//   - map[string]struct{}: the topic resource names bound.
//   - map[string]struct{}: the consumer-group resource names bound.
func provisionAndCollectGrant(t *testing.T, subscriber *model.EventSubscriber) (
	map[string]struct{}, map[string]struct{},
) {
	t.Helper()

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	result, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
	)
	require.NoError(t, err, "provisioning %q must succeed", subscriber.KafkaPrincipal)
	assert.Equal(t, subscriber.KafkaPrincipal, result.Principal)

	entries := requestedACLs(fake)
	require.NotEmpty(t, entries, "a subscriber with a grant must produce bindings")

	topics := map[string]struct{}{}
	groups := map[string]struct{}{}
	for _, entry := range entries {
		assert.Equal(t, "User:"+subscriber.KafkaPrincipal, entry.Principal,
			"every binding must belong to the subscriber being provisioned and to nobody else")

		switch entry.ResourceType {
		case kafka.ResourceTypeTopic:
			topics[entry.ResourceName] = struct{}{}
		case kafka.ResourceTypeGroup:
			groups[entry.ResourceName] = struct{}{}
		default:
			t.Errorf("unexpected resource type %v on %q: only topics and consumer groups are ever bound",
				entry.ResourceType, entry.ResourceName)
		}
	}

	return topics, groups
}

// TestProvisionSubscriberPrincipal_NeverLeaksThePassword is the secret-handling assertion,
// exercised over the whole successful path and over a failing one.
//
// It checks all four escape routes at once: the log message, the structured log fields, the
// serialised result and the returned error.
//
// The logger is turned all the way up to trace for the duration, which is not incidental.
// logrus defaults to info, so a leak written at DEBUG level — the level a developer reaches
// for precisely when they want to see a value while diagnosing something — would never reach
// the hook and this test would pass while the credential was being written to every
// development log. Capturing every level is what closes that hole; the syntax-tree scan
// further down closes the remaining one, which is a path this test never executes.
func TestProvisionSubscriberPrincipal_NeverLeaksThePassword(t *testing.T) {
	captureEveryLogLevel(t)

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

// captureEveryLogLevel raises the standard logger to trace for one test and restores the
// previous level afterwards.
//
// Without it a secret-leak assertion only sees info and above, so a value logged at debug or
// trace level would slip past unnoticed — and debug is exactly the level such a line gets
// written at.
func captureEveryLogLevel(t *testing.T) {
	t.Helper()

	previous := logrus.GetLevel()
	t.Cleanup(func() { logrus.SetLevel(previous) })

	logrus.SetLevel(logrus.TraceLevel)
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
		sentinelPassword,
		"aB3!@#$%^&*()_+-=[]{}|;:',.<>/?`~\"\\",
		"0123456789abcdefghijklmnopqrstuvwxyz",
	}
	for _, password := range valid {
		assert.NoError(t, validateSCRAMPassword(password),
			"printable ASCII of sufficient length and variety must be accepted: %q", password)
	}

	// The values are deliberately unlike any English word: a password that happened to be a
	// substring of the rejection message would make the "must not echo the value" assertion
	// below fail for a reason that has nothing to do with the code.
	// Every value here is long enough to clear the length floor, so each case fails for the
	// alphabet reason it is named for rather than incidentally for its length.
	longEnough := func(seed string) string {
		return seed + "Kq4Zv8Rm2Tb6Wn5Yp3Xj7Hd9Ls1Gf0Ac"
	}

	invalid := map[string]string{
		"empty":          "",
		"trailing space": longEnough("Xk9qZ2") + " ",
		"leading space":  " " + longEnough("Xk9qZ2"),
		"newline":        longEnough("Xk9qZ2") + "\n",
		"tab":            longEnough("Xk9q\tZ2"),
		"null byte":      longEnough("Xk9qZ2\x00"),
		"non-ascii":      longEnough("Xk9qZé2"),
		"delete":         longEnough("Xk9qZ2\x7f"),
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

// TestValidateSCRAMPassword_EnforcesGeneratedStrength is the PASS-01 guard.
//
// A ONE-CHARACTER password used to pass this function and be minted into a real, working
// SCRAM credential holding Read on live ledger topics. A Kafka SASL handshake has no rate
// limit and no lockout, so that credential was not weak, it was open.
//
// Both rules are exercised, and the second is the one that is easy to omit: a length floor on
// its own accepts thirty-two identical characters, which is long and almost entropy-free.
func TestValidateSCRAMPassword_EnforcesGeneratedStrength(t *testing.T) {
	t.Run("a single character is refused", func(t *testing.T) {
		err := validateSCRAMPassword("x")
		require.Error(t, err, "a one-character secret must never be minted into a credential")
		assert.Contains(t, err.Error(), "minimum")
	})

	// Built from the constant rather than written out, so the two boundary cases stay exactly
	// one character apart if the floor is ever changed.
	varied := "Kq4Zv8Rm2Tb6Wn5Yp3Xj7Hd9Ls1Gf0AcJw"
	require.Greater(t, len(varied), MinSCRAMPasswordLength)

	t.Run("one character short of the floor is refused", func(t *testing.T) {
		// The boundary, exactly: a mutation that turned >= into > would pass every other
		// case in this file and fail here.
		short := varied[:MinSCRAMPasswordLength-1]

		require.Error(t, validateSCRAMPassword(short))
	})

	t.Run("exactly the floor is accepted", func(t *testing.T) {
		atFloor := varied[:MinSCRAMPasswordLength]

		require.NoError(t, validateSCRAMPassword(atFloor),
			"the floor must be inclusive, or a correctly generated secret at the boundary is refused")
	})

	t.Run("long but repetitive is refused", func(t *testing.T) {
		padded := strings.Repeat("a", MinSCRAMPasswordLength*2)

		err := validateSCRAMPassword(padded)
		require.Error(t, err, "length without variety is not entropy")
		assert.Contains(t, err.Error(), "distinct")
	})

	t.Run("a padded short secret is refused", func(t *testing.T) {
		// The realistic form of the defect: a real-looking prefix padded out to length.
		err := validateSCRAMPassword("abc" + strings.Repeat("-", MinSCRAMPasswordLength))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "distinct")
	})

	t.Run("the rejection never echoes the secret", func(t *testing.T) {
		for _, password := range []string{"x", strings.Repeat("a", MinSCRAMPasswordLength*2)} {
			err := validateSCRAMPassword(password)
			require.Error(t, err)
			assert.NotContains(t, err.Error(), password,
				"a strength rejection must not quote the value it rejected")
		}
	})
}

// TestProvisionSubscriberPrincipal_RefusesAnyPrincipalItDidNotDerive is the SEC-03 guard on
// the identity half of the boundary.
//
// The principal is not a name, it is the identity the credential is minted for and every ACL
// binding is granted to, so accepting one from a caller is accepting a caller's choice of
// access boundary. Values here were previously only TRIMMED, so every one of these passed.
//
// The wildcard case is the sharpest: Kafka treats the resource name "*" as matching ANY
// resource, and a principal chosen to collide with another subscriber's takes over that
// subscriber's grant. Both are ordinary, authorized requests — nothing about them looks like
// an attack — which is why the refusal has to be structural.
//
// Nothing may be SENT in any of these cases: a refusal that had already written a credential
// would be the AUTH-01 defect arriving through the SEC-03 door.
func TestProvisionSubscriberPrincipal_RefusesAnyPrincipalItDidNotDerive(t *testing.T) {
	derived, err := model.CanonicalKafkaPrincipal(testSubscriberID)
	require.NoError(t, err)

	cases := map[string]string{
		"blank":                       "   ",
		"empty":                       "",
		"a hand-chosen name":          "acme-recon",
		"the wildcard":                "*",
		"a wildcard suffix":           derived + "*",
		"another subscriber's":        "blnk-sub-sub_ffffffff",
		"the namespace alone":         model.SubscriberPrincipalNamespace,
		"correct but with padding":    " " + derived + " ",
		"correct but upper-cased":     strings.ToUpper(derived),
		"correct with a null byte":    derived + "\x00",
		"correct with a group suffix": derived + ".default",
	}

	for name, principal := range cases {
		t.Run(name, func(t *testing.T) {
			fake := newFakeAdminClient()
			admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

			subscriber := testSubscriber()
			subscriber.KafkaPrincipal = principal

			_, err := admin.ProvisionSubscriberPrincipal(
				context.Background(),
				NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
			)
			require.Error(t, err, "%s must be refused", name)
			assert.Contains(t, err.Error(), derived,
				"the refusal must name the only principal this subscriber may hold")
			assert.Zero(t, fake.totalCalls(),
				"nothing may reach the broker when the requested boundary is refused")
		})
	}
}

// TestProvisionSubscriberPrincipal_RefusesAConsumerGroupOutsideTheSubscribersNamespace is the
// SEC-03 guard on the group half of the boundary.
//
// The group binding is PREFIXED, which is what lets a subscriber run several groups without an
// administrative round trip — and what makes a chosen prefix dangerous. A subscriber asking for
// the group "blnk-sub-" would be granted Read on every group whose name starts with it,
// including every other subscriber's: a cross-domain grant obtained through an ordinary
// request.
func TestProvisionSubscriberPrincipal_RefusesAConsumerGroupOutsideTheSubscribersNamespace(t *testing.T) {
	namespace, err := model.CanonicalConsumerGroupNamespace(testSubscriberID)
	require.NoError(t, err)

	cases := map[string]string{
		"a hand-chosen group":            "acme-recon-group",
		"the wildcard":                   "*",
		"the shared namespace prefix":    model.SubscriberPrincipalNamespace,
		"another subscriber's namespace": "blnk-sub-sub_ffffffff.live",
		"the bare namespace":             namespace,
		"a sibling by truncation":        strings.TrimSuffix(namespace, model.SubscriberGroupTerminator),
	}

	for name, group := range cases {
		t.Run(name, func(t *testing.T) {
			fake := newFakeAdminClient()
			admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

			subscriber := testSubscriber()
			subscriber.ConsumerGroupID = group

			_, err := admin.ProvisionSubscriberPrincipal(
				context.Background(),
				NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
			)
			require.Error(t, err, "%s must be refused", name)
			assert.Contains(t, err.Error(), namespace,
				"the refusal must name the namespace the group has to be inside")
			assert.Zero(t, fake.totalCalls())
		})
	}

	t.Run("any leaf inside the namespace is accepted", func(t *testing.T) {
		// The widening the prefixed grant exists for: a replay group beside a live one, with
		// no administrative round trip and still inside the boundary.
		for _, leaf := range []string{"default", "replay", "live-2", "a"} {
			fake := newFakeAdminClient()
			admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

			subscriber := testSubscriber()
			subscriber.ConsumerGroupID = namespace + leaf

			_, err := admin.ProvisionSubscriberPrincipal(
				context.Background(),
				NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
			)
			require.NoError(t, err, "the leaf %q is inside the subscriber's own namespace", leaf)
		}
	})
}

// TestProvisionSubscriberPrincipal_RefusesATopicOutsideTheGrantableAllowlist is the SEC-03
// guard on the topic half of the boundary.
//
// The list becomes the resource name of a LITERAL binding, so whatever is in it is what the
// credential can read. Three classes must be impossible: the wildcard, because "*" matches
// every resource; a foreign topic, because that is somebody else's data on a shared broker;
// and the dead-letter and internal topics, which carry Blnk's own failure metadata and
// diagnostics and have no subscriber audience.
func TestProvisionSubscriberPrincipal_RefusesATopicOutsideTheGrantableAllowlist(t *testing.T) {
	cases := map[string]string{
		"the wildcard":            "*",
		"a foreign topic":         "attacker.transactions",
		"a dead-letter topic":     "blnk.transactions.dlt",
		"the system topic":        "blnk.system",
		"the quarantine topic":    "blnk.quarantine",
		"an internal Kafka topic": "__consumer_offsets",
		"a prefix fragment":       "blnk.",
		"the prefix alone":        "blnk",
	}

	for name, topic := range cases {
		t.Run(name, func(t *testing.T) {
			fake := newFakeAdminClient()
			admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

			subscriber := testSubscriber()
			subscriber.AuthorizedTopics = []string{"blnk.transactions", topic}

			_, err := admin.ProvisionSubscriberPrincipal(
				context.Background(),
				NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
			)
			require.Error(t, err, "%s must be refused", name)
			assert.Contains(t, err.Error(), topic, "the refusal must name the topic it refused")
			assert.Zero(t, fake.totalCalls(),
				"one ungrantable topic must refuse the whole request rather than being dropped silently")
		})
	}
}

// TestProvisionSubscriberPrincipal_ReportsAReplacedCredential proves re-issuing is a
// well-defined operation rather than an error.
//
// It has to be: a subscriber that lost its secret can only be given a new one, and an
// operator needs to know from the result that a working consumer's credential has just
// stopped working.
func TestProvisionSubscriberPrincipal_ReportsAReplacedCredential(t *testing.T) {
	fake := newFakeAdminClient()
	fake.scram[strings.TrimPrefix(testSubscriberPrincipal(t), kafkaPrincipalPrefix)] =
		[]kafka.ScramMechanism{kafka.ScramMechanismSha512}

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	result, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword),
	)
	require.NoError(t, err, "re-issuing a credential must not be an error")
	assert.True(t, result.CredentialReplaced, "replacing an existing credential must be reported")
	assert.Equal(t, 1, fake.callCount("AlterUserScramCredentials"), "the credential must be upserted")

	// The replacement is known to BE a replacement only because provisioning describes the
	// principal before writing. Without that probe the flag could only ever be guessed, and an
	// operator would have no way to tell a new subscriber apart from one whose running
	// consumer just lost its credential.
	assert.Equal(t, 1, fake.callCount("DescribeUserScramCredentials"),
		"provisioning must probe for an existing credential so re-issuance is a defined operation")

	// The same call against a principal that holds nothing must report the opposite, which is
	// what makes the flag informative rather than always true.
	firstIssuance := newFakeAdminClient()
	fresh, err := newTestKafkaAdmin(firstIssuance, MinTopicPartitions, 1).ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword),
	)
	require.NoError(t, err)
	assert.False(t, fresh.CredentialReplaced, "a principal that held nothing must not report a replacement")
	assert.Equal(t, 1, firstIssuance.callCount("DescribeUserScramCredentials"))
}

// TestProvisionSubscriberPrincipal_RefusesWhenTheBrokerEnforcesNothing is the SEC-02 guard.
//
// This test used to require the opposite: that provisioning SUCCEED against a broker with no
// authorizer, on the reasoning that refusing would make Blnk unusable there. What that
// actually produced was a working credential with cluster-wide read access to every ledger
// topic, every dead-letter topic and every other subscriber's data — returned to the caller
// as a success, with the only trace a log line in a successful provisioning nobody reads.
//
// A KRaft broker without authorizer.class.name accepts every ACL binding and applies none. So
// there is no such thing as issuing a bounded credential on it, and "usable" is the wrong
// property to optimise: the broker must be fixed.
//
// The refusal is asserted to happen BEFORE anything is written, which is the whole ordering
// half of the finding — a refusal after the upsert would leave exactly the credential it was
// trying to prevent.
func TestProvisionSubscriberPrincipal_RefusesWhenTheBrokerEnforcesNothing(t *testing.T) {
	hook := logtest.NewGlobal()
	defer hook.Reset()

	fake := newFakeAdminClient()
	fake.securityDisabled = true

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	result, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword),
	)
	require.Error(t, err, "a broker that enforces no ACLs must not have a credential issued against it")
	assert.ErrorIs(t, err, ErrAuthorizerNotEnforcing,
		"the refusal must be recognisable without matching message text")
	assert.Contains(t, err.Error(), "StandardAuthorizer",
		"the error must name the broker setting that fixes it")

	assert.False(t, result.AuthorizerActive, "the finding must be reported to the caller")
	assert.False(t, result.CredentialWritten, "no credential may exist")
	assert.Zero(t, fake.callCount("AlterUserScramCredentials"),
		"the probe must run BEFORE the credential is written, not after")
	assert.Zero(t, fake.callCount("CreateACLs"), "and before any binding is attempted")
	assert.Empty(t, fake.scram, "the broker must hold no credential for this principal")

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

// TestProvisionSubscriberPrincipal_RefusesWhenEnforcementCannotBeConfirmed is the second half
// of SEC-02, and it is the case that is tempting to let through.
//
// The probe needs the administrative principal to be allowed to describe ACLs. When it is not
// — or when the broker cannot be reached — the honest answer is "I do not know whether
// anything is being enforced", and that was previously treated as a reason to log and carry
// on. From the point of view of the credential about to be minted, unverifiable enforcement
// and absent enforcement are indistinguishable, so both must refuse.
func TestProvisionSubscriberPrincipal_RefusesWhenEnforcementCannotBeConfirmed(t *testing.T) {
	fake := newFakeAdminClient()
	fake.transportErrors["DescribeACLs"] = errors.New("broker unreachable")

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	result, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword),
	)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrAuthorizerNotEnforcing)
	assert.Contains(t, err.Error(), "Describe",
		"the error must say what the administrative principal needs, since that is the usual cause")

	assert.False(t, result.CredentialWritten)
	assert.Zero(t, fake.callCount("AlterUserScramCredentials"),
		"an unanswerable probe must stop provisioning before the credential is written")
	assert.Empty(t, fake.scram)
}

// TestProvisionSubscriberPrincipal_RevokesTheCredentialWhenBindingFails is the AUTH-01
// compensation guard.
//
// Provisioning writes the credential first, because a binding for a principal that does not
// exist is inert while a credential without bindings AUTHENTICATES. So a binding failure used
// to leave a live, valid credential that had been generated and returned by a call which then
// reported failure — a secret in a subscriber's hands that no registry row records.
//
// The compensation revokes it. Both halves are asserted, because either alone leaves the
// system in a state somebody has to clean up by hand.
func TestProvisionSubscriberPrincipal_RevokesTheCredentialWhenBindingFails(t *testing.T) {
	fake := newFakeAdminClient()
	fake.aclErrors = []error{errors.New("binding rejected")}

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	result, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword),
	)
	require.Error(t, err, "a failed binding must fail the provisioning")

	assert.True(t, result.Compensated, "the caller must be told the broker was left clean")
	assert.False(t, result.CredentialWritten,
		"CredentialWritten must describe the state the broker is actually left in, not the step that ran")
	assert.Empty(t, fake.scram,
		"the credential written before the binding failed must not survive the failure")
	assert.NotZero(t, fake.callCount("DeleteACLs"),
		"the bindings that may have landed must be removed too, since CreateACLs is not atomic")
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
	// The plaintext is unreachable from outside the package by design, so the assertion is
	// on what IS observable: a secret of exactly the expected length is held. Reading the
	// value back would be the leak the type exists to prevent.
	assert.Equal(t, len(sentinelPassword), request.Password.Len(),
		"the password must be carried through unchanged")
	assert.False(t, request.Password.IsZero(), "a secret must be present")

	for _, entry := range request.aclEntries() {
		if entry.ResourceType != kafka.ResourceTypeTopic {
			continue
		}
		assert.NotEqual(t, *subscriber.PartitionKeyPrefix, entry.ResourceName,
			"the partition-key prefix must never become a topic resource name")
		assert.Equal(t, kafka.PatternTypeLiteral, entry.ResourcePatternType,
			"the partition-key prefix must never be expressed as a prefixed topic pattern")
	}

	assert.Equal(t, SubscriberProvisioningRequest{Password: NewSubscriberSecret(sentinelPassword)},
		NewSubscriberProvisioningRequest(nil, sentinelPassword),
		"a nil registry row must produce a request that fails validation rather than a panic")
}

// TestSubscriberProvisioningRequest_NormalisesItsInputs covers the small normalisations the
// bindings depend on, and the one value that is DERIVED rather than normalised.
//
// The consumer group namespace is the derived one: trimming a caller's group prefix would
// still be honouring a caller's choice of boundary, and a prefixed grant over a chosen string
// is exactly how one subscriber reaches another's groups. The principal is trimmed only so
// that validate can report a whitespace mismatch as a mismatch of names.
func TestSubscriberProvisioningRequest_NormalisesItsInputs(t *testing.T) {
	request := SubscriberProvisioningRequest{
		SubscriberID:        testSubscriberID,
		Principal:           "  acme-recon  ",
		ConsumerGroupPrefix: "  acme-group ",
		Topics:              []string{" blnk.transactions ", "", "blnk.transactions", "blnk.balances"},
	}

	assert.Equal(t, "acme-recon", request.principal())
	assert.Equal(t, testSubscriberGroupNamespace(t), request.consumerGroupPrefix(),
		"the group namespace is derived from the subscriber id, never taken from the request")
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

	fake.mu.Lock()
	described := fake.describeScramRequests
	fake.mu.Unlock()

	// The probe must name exactly the principal it was asked about. An empty user list is
	// how kafka-go asks about EVERY principal in the cluster, which would answer a different
	// question, cost far more, and require a broader administrative grant than provisioning
	// one subscriber needs.
	require.Len(t, described, 1, "one probe, one round trip")
	require.Len(t, described[0].Users, 1,
		"the describe request must name exactly one principal, never the whole cluster")
	assert.Equal(t, "has-sha512", described[0].Users[0].Name)

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

	// The group id is the DERIVED one, because that is the only kind the collector will
	// ever hand to this method: a registry row whose identifiers are not generated is
	// skipped before it is measured. Its leaf ("default") is deliberately absent from the
	// gauge assertion below — see the group-label expectation.
	group, err := model.CanonicalConsumerGroupID("sub_0f6e2c8a")
	require.NoError(t, err)
	require.Equal(t, "blnk-sub-sub_0f6e2c8a.default", group,
		"the derived group shape is what the gauge label is resolved against")

	report, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{
		SubscriberID: "sub_0f6e2c8a",
		GroupID:      group,
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
		// The SUBSCRIBER-SCOPED ROOT, not the full group id. A subscriber that runs
		// several consumer groups is one subscriber to an operator triaging the alert,
		// and collapsing the leaf is what keeps its lag on one series instead of one
		// series per group it happens to be running.
		assert.Equal(t, "blnk-sub-sub_0f6e2c8a", record.attributes["group"],
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

	fake.mu.Lock()
	fetch := fake.offsetFetchRequests[0]
	fake.mu.Unlock()

	// The group is the whole subject of the measurement: an OffsetFetch aimed at the wrong
	// group returns a perfectly valid answer about somebody else, and the lag figure would be
	// wrong without a single error anywhere.
	assert.Equal(t, "acme-recon-group", fetch.GroupID,
		"committed offsets must be read for the requested consumer group and no other")
	assert.Equal(t, []int{0, 1}, fetch.Topics["blnk.transactions"],
		"every partition of the measured topic must be included in the commit fetch")
}

// TestConsumerLag_CarriesAlertScaleMagnitudesWithoutOverflowOrTruncation pins the number at
// the scale the alert actually fires at.
//
// alerts/blnk-kafka-alerts.yml fires SubscriberConsumerLagHigh on
// blnk_kafka_consumer_lag > 10000, so a measurement that saturated, truncated or wrapped
// anywhere below that would DISABLE the alert rather than trip it — and silently, because a
// smaller-than-true lag is indistinguishable from a healthy consumer. Every figure below
// therefore sits above the threshold, and the second case sits high in the int64 range that
// Kafka offsets and the gauge both use.
func TestConsumerLag_CarriesAlertScaleMagnitudesWithoutOverflowOrTruncation(t *testing.T) {
	// alertThreshold mirrors the rule file. It is written out here rather than imported
	// because the rule is PromQL and not Go: asserting the number on both sides is the only
	// way the two can be kept in agreement.
	const alertThreshold = int64(10_000)

	t.Run("an aggregate above the alert threshold is reported exactly", func(t *testing.T) {
		gauge := captureConsumerLagGauge(t)

		// Six partitions — the required minimum — each individually below the threshold. The
		// alert reads the AGGREGATE, so per-partition figures that each look survivable must
		// still sum to a firing value; a per-partition comparison would never fire here.
		const endOffset = int64(10_000)
		const perPartitionLag = int64(4_000)

		fake := newFakeAdminClient()
		for partition := 0; partition < MinTopicPartitions; partition++ {
			fake.withOffsets("blnk.transactions", partition, 0, endOffset)
			fake.withCommitted("blnk.transactions", partition, endOffset-perPartitionLag)
		}

		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		report, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{
			SubscriberID: "sub_0f6e2c8a",
			GroupID:      "acme-recon-group",
			Topics:       []string{"blnk.transactions"},
		})
		require.NoError(t, err)

		const expected = perPartitionLag * MinTopicPartitions
		require.Greater(t, int64(expected), alertThreshold,
			"the fixture itself must exceed the alert threshold, or this test proves nothing")

		assert.Equal(t, int64(24_000), report.TotalLag,
			"every one of the six partitions must contribute its full four thousand")
		require.Len(t, report.Topics, 1)
		assert.Equal(t, int64(24_000), report.Topics[0].TotalLag)
		assert.Len(t, report.Topics[0].Partitions, MinTopicPartitions,
			"a partition dropped from the sum would understate the lag")

		records := gauge.snapshot()
		require.Len(t, records, 1, "one topic measured, one gauge value published")
		assert.Equal(t, int64(24_000), records[0].value,
			"the gauge must carry the aggregate undiminished; a truncated value would silence the alert")
		assert.Greater(t, records[0].value, alertThreshold,
			"the published value must cross the threshold the rule fires on")
	})

	t.Run("offsets near the int64 ceiling neither wrap nor saturate", func(t *testing.T) {
		gauge := captureConsumerLagGauge(t)

		// Kafka offsets are int64 and a long-lived partition genuinely grows without bound.
		// The instrument is an Int64Gauge, so the only remaining failure mode is arithmetic:
		// an int or int32 anywhere in the chain would wrap this into a negative number, and a
		// negative contribution would drag the summed figure below the truth.
		const endOffset = int64(1) << 62
		const behind = int64(1_500_000)

		fake := newFakeAdminClient()
		fake.withOffsets("blnk.transactions", 0, 0, endOffset).
			withCommitted("blnk.transactions", 0, endOffset-behind)
		fake.withOffsets("blnk.balances", 0, 0, endOffset).
			withCommitted("blnk.balances", 0, endOffset-behind)

		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		report, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{
			SubscriberID: "sub_0f6e2c8a",
			GroupID:      "acme-recon-group",
			Topics:       []string{"blnk.transactions", "blnk.balances"},
		})
		require.NoError(t, err)

		require.Len(t, report.Topics, 2)
		assert.Equal(t, behind, report.Topics[0].TotalLag,
			"the difference between two very large offsets must be exact, not approximate")
		assert.Equal(t, behind, report.Topics[1].TotalLag)
		assert.Equal(t, behind*2, report.TotalLag, "the cross-topic sum must not overflow")
		assert.Greater(t, report.TotalLag, alertThreshold)

		records := gauge.snapshot()
		require.Len(t, records, 2)
		for _, record := range records {
			assert.Equal(t, behind, record.value,
				"the gauge must receive the exact figure at this scale")
			assert.GreaterOrEqual(t, record.value, int64(0),
				"a wrapped or saturated value would surface as a negative gauge reading")
		}

		// The arithmetic itself, at the scale a 32-bit accumulator could not survive: an
		// uncommitted partition at the int64 ceiling must report full lag at that ceiling.
		assert.Equal(t, endOffset, lagForPartition(-1, 0, endOffset),
			"full lag on a partition at the int64 scale must be reported at that scale")
		assert.Equal(t, endOffset-1, lagForPartition(1, 0, endOffset),
			"a single committed record at the int64 scale must reduce the lag by exactly one")
	})

	t.Run("the gauge the alert reads is the one internal metrics declares", func(t *testing.T) {
		// The rule names the Prometheus series blnk_kafka_consumer_lag, which is the
		// OpenTelemetry instrument blnk.kafka.consumer_lag after the exporter's
		// dot-to-underscore mapping. A rename on either side leaves a rule that matches
		// nothing and reports itself permanently healthy, so the name is asserted against its
		// declaration rather than assumed.
		declaration, err := os.ReadFile(
			filepath.Join(moduleRootDir(t), "internal", "metrics", "metrics.go"),
		)
		require.NoError(t, err, "internal/metrics/metrics.go must be readable")

		assert.Contains(t, string(declaration), `"blnk.kafka.consumer_lag"`,
			"the consumer-lag gauge must be declared under the exact name the alert rule reads")

		rule, err := os.ReadFile(
			filepath.Join(moduleRootDir(t), "alerts", "blnk-kafka-alerts.yml"),
		)
		require.NoError(t, err, "alerts/blnk-kafka-alerts.yml must be readable")

		assert.Contains(t, string(rule), "blnk_kafka_consumer_lag > 10000",
			"the alert must fire above ten thousand messages, which is the magnitude this "+
				"measurement has to represent without truncation")
	})
}

// TestTopicEndOffsets_DefaultsToTheWholeInventory pins the reconciliation's default scope.
//
// The daily check compares outbox counts against the summed offsets of every topic Blnk
// owns, so an unnamed call must cover exactly the whole topic inventory — no more, and
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

	// The figure has to come from the broker's own offsets. ListOffsets is the only request
	// that reports them, so a reconciliation built on anything else — a cached count, a
	// consumer's own position — would be comparing the outbox against itself.
	assert.Equal(t, 1, fake.callCount("ListOffsets"),
		"end offsets must be read from the broker with ListOffsets, in one request")
	assert.Zero(t, fake.callCount("OffsetFetch"),
		"the reconciliation is about what was published, not about what any group consumed")

	// Every topic in the inventory must appear in the per-topic reduction, which is the shape
	// GET /events/stats reports and the runbook sums.
	endOffsets := report.EndOffsetsByTopic()
	require.Len(t, endOffsets, len(expectedEventTopics))
	for _, topic := range expectedEventTopics {
		assert.Equal(t, int64(1), endOffsets[topic],
			"topic %q must contribute its own end offset to the reconciliation", topic)
	}
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
			Brokers:         []string{" broker-1:9092 ", "", "broker-2:9092"},
			SASLAdminUser:   "admin",
			SASLAdminSecret: "REDACTED_TEST_SECRET",
			MinPartitions:   2,
			// The local single-broker stack listens on SASL_PLAINTEXT, and the transport
			// refuses to dial unencrypted without this acknowledgement — see
			// TestNewKafkaAdmin_RefusesToDialAnUnencryptedBrokerByDefault.
			InsecureLocalDev:  true,
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
			InsecureLocalDev:  true,
			ReplicationFactor: 1,
		},
	})
	require.Error(t, err)
	assert.Nil(t, admin)
	assert.Contains(t, err.Error(), "admin", "the error must name which principal is half-configured")
	assert.Contains(t, err.Error(), "secret", "and which half is missing")
}

// TestNewKafkaAdmin_RefusesToDialAnUnencryptedBrokerByDefault is the CRYPTO-01 guard on the
// ADMINISTRATIVE transport, which is the one that matters most.
//
// This client's requests carry SCRAM credentials FOR OTHER PRINCIPALS: a subscriber's salted
// password crosses the wire inside an AlterUserScramCredentials request. So an unencrypted
// administrative connection does not expose one deployment's data, it exposes every
// subscriber's credential at the moment it is minted.
//
// The refusal shares its implementation with the publisher's transport, which is the point of
// the finding: two hand-rolled transports could disagree about whether TLS was required, and
// one of them being right was not enough.
func TestNewKafkaAdmin_RefusesToDialAnUnencryptedBrokerByDefault(t *testing.T) {
	admin, err := NewKafkaAdmin(&config.Configuration{
		Kafka: config.KafkaConfig{
			Brokers:           []string{"broker-1:9092"},
			SASLAdminUser:     "admin",
			SASLAdminSecret:   "REDACTED_TEST_SECRET",
			ReplicationFactor: 1,
		},
	})
	require.Error(t, err, "an unacknowledged plaintext administrative connection must be refused")
	assert.Nil(t, admin)
	assert.Contains(t, err.Error(), "KAFKA_TLS_ENABLED")
	assert.Contains(t, err.Error(), "KAFKA_INSECURE_LOCAL_DEV")
}

// TestNewKafkaAdmin_ReportsEveryTransportProblemAtOnce covers the diagnostics, which is where
// a security check most easily becomes an operational nuisance.
//
// A deployment that has neither enabled TLS nor finished configuring its credentials has two
// problems. Reporting one sends the operator round the loop twice — fix the encryption,
// restart, then learn about the credential — so both are reported together.
func TestNewKafkaAdmin_ReportsEveryTransportProblemAtOnce(t *testing.T) {
	_, err := NewKafkaAdmin(&config.Configuration{
		Kafka: config.KafkaConfig{
			Brokers:           []string{"broker-1:9092"},
			SASLAdminUser:     "admin",
			ReplicationFactor: 1,
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "KAFKA_TLS_ENABLED", "the encryption problem must be reported")
	assert.Contains(t, err.Error(), "secret", "and the credential problem in the same failure")
}

// TestNewKafkaAdmin_AllowsAPlaintextBrokerWithoutSASL keeps a broker with no SASL listener
// usable: attaching a mechanism it does not offer would fail the handshake.
func TestNewKafkaAdmin_AllowsAPlaintextBrokerWithoutSASL(t *testing.T) {
	admin, err := NewKafkaAdmin(&config.Configuration{
		Kafka: config.KafkaConfig{
			Brokers:           []string{"broker-1:9092"},
			MinPartitions:     MinTopicPartitions,
			InsecureLocalDev:  true,
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
		Kafka: config.KafkaConfig{
			Brokers:           []string{"broker-1:9092"},
			InsecureLocalDev:  true,
			ReplicationFactor: 1,
		},
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
// proves it cannot leak on ANY path, including one added later, by checking that no logging,
// error-formatting, trace-attribute or metric-label call anywhere in the file takes the
// password as an argument.
//
// The four call families are covered together because they are one hazard wearing four
// faces: a log line, an error message, a span attribute and a gauge label all end up
// somewhere an operator — or an exported telemetry backend — can read.
//
// The derivation call is deliberately NOT in the forbidden set. pbkdf2 has to be handed the
// plaintext; that is the one place it legitimately goes, and the value that comes out is
// one-way.
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
		// Error construction and string formatting: the classic way a secret reaches an
		// operator, by being interpolated into a message that is then returned or logged.
		case "New", "Errorf", "Sprintf", "Sprint", "Sprintln",
			"Fprint", "Fprintf", "Fprintln", "Print", "Printf", "Println":
			return true

		// logrus, at every level, plus its structured-field builders.
		case "Debug", "Debugf", "Debugln", "Info", "Infof", "Infoln",
			"Warn", "Warnf", "Warnln", "Warning", "Warningf",
			"Error", "Errorln", "Fatal", "Fatalf", "Fatalln", "Panic", "Panicf", "Panicln",
			"WithField", "WithFields", "WithError", "WithContext":
			return true

		// Trace attributes and metric labels. These are named explicitly because they are
		// the least obvious escape route and the easiest to add without thinking: a span
		// attribute or a gauge label carrying the password publishes it to every backend the
		// telemetry pipeline exports to, and nothing about the call site looks like logging.
		case "SetAttributes", "WithAttributes", "AddEvent", "RecordError", "SetStatus",
			"Record", "Add", "Observe",
			"String", "StringSlice", "Int", "IntValue", "Int64", "Float64", "Bool", "Any":
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

// TestAdminResultTypes_HaveNowhereToPutTheSecret closes the last escape route the runtime
// and syntax-tree tests cannot see: a return value.
//
// A single field is all it would take. SubscriberProvisioningResult is logged, and it is
// serialised into the credential-endpoint response, so a "Password" or "Secret" field added
// to it later would disclose the credential on every issuance without one logging call being
// written and without the syntax-tree scan above noticing anything. The same goes for the
// reports the measurement methods return, which the statistics endpoint serialises.
//
// The second half asserts that no method hands back the REQUEST, which does legitimately
// carry the plaintext: an accessor returning it — a "LastProvisioned" style getter, say —
// would put the secret straight into a caller's hands and, from there, into whatever the
// caller logs.
func TestAdminResultTypes_HaveNowhereToPutTheSecret(t *testing.T) {
	// Deliberately does NOT include "credential": CredentialReplaced is a legitimate boolean
	// that reports a replacement without carrying anything. The words listed are the ones
	// that could only ever name the value itself.
	forbiddenFragments := []string{"password", "secret", "passphrase", "plaintext"}

	returned := []interface{}{
		SubscriberProvisioningResult{},
		TopicAssurance{},
		TopicAssuranceReport{},
		ConsumerLagReport{},
		TopicLag{},
		PartitionLag{},
		TopicOffsetReport{},
		TopicOffsetSnapshot{},
		PartitionOffsetSnapshot{},
		OutboxReconciliation{},
	}

	for _, value := range returned {
		valueType := reflect.TypeOf(value)

		for index := 0; index < valueType.NumField(); index++ {
			field := strings.ToLower(valueType.Field(index).Name)

			for _, fragment := range forbiddenFragments {
				assert.NotContains(t, field, fragment,
					"%s.%s: a returned type must have no field that could carry the SCRAM password, "+
						"because these values are logged and serialised",
					valueType.Name(), valueType.Field(index).Name)
			}
		}
	}

	// The request is the one type that holds the plaintext, which is exactly why it must
	// never come back out.
	requestType := reflect.TypeOf(SubscriberProvisioningRequest{})
	require.NotEqual(t, -1, fieldIndexNamed(requestType, "Password"),
		"the request is expected to carry the password; if it stopped doing so this test would "+
			"be guarding nothing")

	adminType := reflect.TypeOf((*KafkaAdminClient)(nil))
	for index := 0; index < adminType.NumMethod(); index++ {
		method := adminType.Method(index)

		for out := 0; out < method.Type.NumOut(); out++ {
			assert.NotEqual(t, requestType, method.Type.Out(out),
				"%s returns the provisioning request, which carries the plaintext password",
				method.Name)
		}
	}
}

// fieldIndexNamed returns the index of a struct field by name, or -1 when it is absent.
func fieldIndexNamed(structType reflect.Type, name string) int {
	for index := 0; index < structType.NumField(); index++ {
		if structType.Field(index).Name == name {
			return index
		}
	}

	return -1
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
	assert.Contains(t, imports, "github.com/blnkfinance/blnk/internal/metrics",
		"the consumer-lag figure must feed the shared gauge")

	// sasl/scram is deliberately NOT imported here any more, and that absence is the CRYPTO-01
	// fix rather than an omission. The transport — TLS policy, plaintext refusal, credential
	// validation and the SCRAM mechanism — is built by NewKafkaTransport, which the publisher
	// shares. Two hand-rolled transports is one implementation too many of a single security
	// decision, and this is the client whose requests carry OTHER PRINCIPALS' credentials, so
	// it must not be the one that gets it wrong on its own.
	assert.NotContains(t, imports, "github.com/segmentio/kafka-go/sasl/scram",
		"the administrative transport must be built by the shared NewKafkaTransport, not assembled here")

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
		"WriteMessages": "producing belongs to the publisher, and only the relay may write events",
		"Writer":        "an admin client that could also produce would make it possible to bypass the outbox",
		"Reader":        "consuming, consumer error handling and subscriber-side dead-lettering are out of scope",
		"Command":       "provisioning is native Go; shelling out would need a JVM on every image",
	}
	for name, reason := range forbiddenReferences {
		_, present := referenced[name]
		assert.False(t, present, "event_admin.go must not reference %s: %s", name, reason)
	}

	// DeleteACLs is REQUIRED here, and this assertion used to forbid it.
	//
	// The exclusion was made on the same "no destructive operations" reasoning as DeleteTopics,
	// and that conflated two different kinds of destruction. Deleting a topic destroys committed
	// events irreversibly. Deleting an ACL binding removes an authorization that the registry
	// row can recreate — the registry, not the broker, is the record of what a subscriber may
	// read.
	//
	// Excluding it meant Blnk could grant access and never withdraw it: reducing a subscriber's
	// topics left the wider grant standing, and deleting a subscriber left its whole boundary
	// live with no row left to describe it. The safe-looking omission produced the less safe
	// system — a set of permissions that only ever grew.
	assert.Contains(t, referenced, "DeleteACLs",
		"revoking a boundary is part of the lifecycle; without it a grant can only ever widen")
	assert.Contains(t, referenced, "Deletions",
		"SCRAM credential deletion is what actually ends access, because it is what the SASL "+
			"handshake checks")

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

// ---------------------------------------------------------------------------------------
// Topic geometry — TOPIC-01
// ---------------------------------------------------------------------------------------

// TestEnsureTopics_RefusesToGrowATopicThatHoldsRecords is the TOPIC-01 guard on partition
// growth.
//
// Growth was previously unconditional, and the cost is not recoverable. The partition a key
// lands on is murmur2(key) mod partitionCount, so raising the count RE-MAPS keys: a ledger
// that hashed into partition 2 of one lands elsewhere out of six, and its history is split
// across two partitions with no ordering between them. Every key already written loses the
// per-aggregate ordering guarantee, and the events cannot be moved back.
//
// So a topic that holds records is refused, and the refusal is loud: EnsureTopics returns
// ErrPartitionGrowthRefused rather than reporting a quiet success, because the deployment now
// has a topic that cannot satisfy the configured geometry and only a planned migration fixes
// it.
func TestEnsureTopics_RefusesToGrowATopicThatHoldsRecords(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	// One partition holding four records: exactly the legacy topic this check exists for.
	fake.withTopic("blnk.transactions", 1).withOffsets("blnk.transactions", 0, 0, 4)
	for _, topic := range AllTopicsWithDeadLetters() {
		if topic != "blnk.transactions" {
			fake.withTopic(topic, MinTopicPartitions)
		}
	}

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.EnsureTopics(context.Background())
	require.Error(t, err, "a refused growth must be reported as an error, not as a quiet success")
	assert.ErrorIs(t, err, ErrPartitionGrowthRefused)
	assert.Contains(t, err.Error(), "blnk.transactions", "the error must name the topic")
	assert.Contains(t, err.Error(), "KAFKA_ALLOW_PARTITION_GROWTH",
		"and the flag an operator sets once the migration is planned")

	assert.Zero(t, fake.callCount("CreatePartitions"),
		"no partition may be added to a topic that holds records")

	assert.Equal(t, 1, report.GrowthRefusedCount)
	entry, found := report.Lookup("blnk.transactions")
	require.True(t, found)
	assert.True(t, entry.GrowthRefused)
	assert.Equal(t, 1, entry.PartitionsAfter,
		"the report must describe the topic as it still is, not as configuration wants it")
	assert.False(t, entry.PartitionsAdded)
}

// TestEnsureTopics_GrowsAnEmptyTopic keeps the safe case working.
//
// A topic with no records has no key-to-partition mapping to preserve, so growing it costs
// nothing and is exactly what an under-provisioned but unused topic needs.
func TestEnsureTopics_GrowsAnEmptyTopic(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	fake.withTopic("blnk.transactions", 1)
	for _, topic := range AllTopicsWithDeadLetters() {
		if topic != "blnk.transactions" {
			fake.withTopic(topic, MinTopicPartitions)
		}
	}

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.EnsureTopics(context.Background())
	require.NoError(t, err)

	assert.Equal(t, 1, report.GrownCount)
	assert.Zero(t, report.GrowthRefusedCount)

	fake.mu.Lock()
	requests := fake.createPartitionsRequests
	fake.mu.Unlock()
	require.Len(t, requests, 1)
	require.Len(t, requests[0].Topics, 1)
	assert.Equal(t, "blnk.transactions", requests[0].Topics[0].Name)
	assert.Equal(t, int32(MinTopicPartitions), requests[0].Topics[0].Count,
		"CreatePartitions takes the new TOTAL, not a delta")
}

// TestEnsureTopics_GrowsALiveTopicOnlyWhenExplicitlyPermitted covers the deliberate escape
// hatch.
//
// It exists for the operator who HAS planned the migration and wants the pass to perform the
// growth step. It must warn while doing it, naming the record count it is about to re-map,
// because the consequence is not reversible and the flag may outlive the intent that set it.
func TestEnsureTopics_GrowsALiveTopicOnlyWhenExplicitlyPermitted(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	hook := logtest.NewGlobal()
	defer hook.Reset()

	fake := newFakeAdminClient()
	fake.withTopic("blnk.transactions", 1).withOffsets("blnk.transactions", 0, 0, 9)
	for _, topic := range AllTopicsWithDeadLetters() {
		if topic != "blnk.transactions" {
			fake.withTopic(topic, MinTopicPartitions)
		}
	}

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)
	admin.allowPartitionGrowth = true

	report, err := admin.EnsureTopics(context.Background())
	require.NoError(t, err, "an explicitly permitted growth must proceed")
	assert.Equal(t, 1, report.GrownCount)
	assert.Zero(t, report.GrowthRefusedCount)

	var warned bool
	for _, entry := range hook.AllEntries() {
		if entry.Level <= logrus.WarnLevel && strings.Contains(entry.Message, "re-map") {
			warned = true

			break
		}
	}
	assert.True(t, warned,
		"permitting the growth must still warn that existing events lose their ordering guarantee")
}

// TestEnsureTopics_TreatsUnreadableOffsetsAsOccupied covers the direction the uncertainty must
// resolve in.
//
// If the broker will not report a partition's offsets, the pass does not know whether the
// topic holds records. Assuming "empty" would authorise the one operation that cannot be
// undone, so the unknown is treated as occupied and the growth is refused.
func TestEnsureTopics_TreatsUnreadableOffsetsAsOccupied(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	fake.withTopic("blnk.transactions", 1)
	fake.offsetErrors["blnk.transactions"] = map[int]error{0: kafka.LeaderNotAvailable}
	for _, topic := range AllTopicsWithDeadLetters() {
		if topic != "blnk.transactions" {
			fake.withTopic(topic, MinTopicPartitions)
		}
	}

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	_, err := admin.EnsureTopics(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrPartitionGrowthRefused,
		"an unanswerable offset read must not be read as an empty topic")
	assert.Zero(t, fake.callCount("CreatePartitions"))
}

// TestEnsureTopics_ReportsAnUnderReplicatedExistingTopic is the TOPIC-01 guard on durability.
//
// The configured factor was applied to topics this pass CREATED and discarded from the
// metadata of topics that already existed. So a topic sitting at one replica was reported as
// assured under a configuration asking for three: the report said the durability requirement
// was met, the cluster did not meet it, and losing one broker would have taken the events.
//
// The observed count is now recorded and compared, and the error says what an operator has to
// do — a factor cannot be raised by re-running assurance, only by reassigning partitions.
func TestEnsureTopics_ReportsAnUnderReplicatedExistingTopic(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	for _, topic := range AllTopicsWithDeadLetters() {
		fake.withReplicas(topic, MinTopicPartitions, 3)
	}
	// One topic left at a single replica, which is the realistic shape: it was created by
	// hand, or auto-created, before the configuration asked for three.
	fake.withReplicas("blnk.transactions", MinTopicPartitions, 1)

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 3)

	report, err := admin.EnsureTopics(context.Background())
	require.Error(t, err, "an under-replicated topic must not be reported as assured")
	assert.ErrorIs(t, err, ErrReplicationFactorInadequate)
	assert.Contains(t, err.Error(), "blnk.transactions")
	assert.Contains(t, err.Error(), "reassign",
		"the error must say that re-running assurance cannot fix an existing topic's factor")

	entry, found := report.Lookup("blnk.transactions")
	require.True(t, found)
	assert.True(t, entry.ReplicationInadequate)
	assert.Equal(t, 1, entry.ReplicationFactor,
		"the report must carry the OBSERVED replica count, not the configured one")

	healthy, found := report.Lookup("blnk.balances")
	require.True(t, found)
	assert.False(t, healthy.ReplicationInadequate)
	assert.Equal(t, 3, healthy.ReplicationFactor)
}

// TestEnsureTopics_ReportsEveryGeometryProblemTogether covers the diagnostics.
//
// An operator fixing topic geometry needs to see every problem at once: fixing the replication
// and re-running only to discover the partition refusal is two maintenance windows where one
// would do.
func TestEnsureTopics_ReportsEveryGeometryProblemTogether(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	for _, topic := range AllTopicsWithDeadLetters() {
		fake.withReplicas(topic, MinTopicPartitions, 3)
	}
	fake.withReplicas("blnk.balances", MinTopicPartitions, 1)
	fake.withReplicas("blnk.transactions", 1, 3).withOffsets("blnk.transactions", 0, 0, 2)

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 3)

	_, err := admin.EnsureTopics(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrReplicationFactorInadequate, "the replication problem must be reported")
	assert.ErrorIs(t, err, ErrPartitionGrowthRefused, "and the partition problem in the same failure")
}

// ---------------------------------------------------------------------------------------
// Revocation and lifecycle — AUTH-01
// ---------------------------------------------------------------------------------------

// TestRevokeSubscriber_RemovesTheBoundaryBeforeTheIdentity is the AUTH-01 guard on
// deprovisioning.
//
// The administrative contract used to expose creation and no removal at all, so reducing a
// subscriber's topics or deleting the subscriber entirely left the credential and its bindings
// live at the broker: the registry row was gone and the access was not, and nothing in Blnk
// could see it any more.
//
// The ORDER is asserted, not just the effect. Bindings must go before the credential: reversed,
// there is a window in which the principal cannot authenticate while its bindings still stand,
// and recreating the credential — by a retry, or by an operator — silently restores the old
// boundary.
func TestRevokeSubscriber_RemovesTheBoundaryBeforeTheIdentity(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)
	subscriber := testSubscriber()

	_, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
	)
	require.NoError(t, err)
	require.NotEmpty(t, fake.scram, "the fixture must start from a provisioned principal")

	require.NoError(t, admin.RevokeSubscriber(context.Background(), subscriber))

	assert.Empty(t, fake.scram, "revocation must delete the credential, which is what ends access")

	fake.mu.Lock()
	deleteRequests := fake.deleteACLsRequests
	calls := append([]string(nil), fake.calls...)
	fake.mu.Unlock()

	require.Len(t, deleteRequests, 1)
	assert.Len(t, deleteRequests[0].Filters, 5,
		"exactly the five bindings provisioning created must be removed, no more")

	// Every filter must be exact rather than a broad "everything for this principal" match: a
	// filter wide enough to catch a subscriber's bindings is wide enough to catch an operator's,
	// and ACL deletion has no undo.
	for _, filter := range deleteRequests[0].Filters {
		assert.Equal(t, testSubscriberPrincipal(t), filter.PrincipalFilter)
		assert.NotEqual(t, kafka.ACLOperationTypeAny, filter.Operation,
			"a filter must name the operation it removes")
		assert.NotEqual(t, kafka.ACLPermissionTypeAny, filter.PermissionType)
		assert.NotEqual(t, kafka.PatternTypeAny, filter.ResourcePatternTypeFilter)
		assert.NotEmpty(t, filter.ResourceNameFilter)
	}

	deleteIndex, credentialIndex := -1, -1
	for i, call := range calls {
		if call == "DeleteACLs" && deleteIndex < 0 {
			deleteIndex = i
		}
		if call == "AlterUserScramCredentials" {
			credentialIndex = i // the last one, which is the deletion
		}
	}
	require.NotEqual(t, -1, deleteIndex)
	require.NotEqual(t, -1, credentialIndex)
	assert.Less(t, deleteIndex, credentialIndex,
		"the boundary must be removed before the identity, so a partial failure leaves fewer rights")
}

// TestRevokeSubscriberPrincipal_IsIdempotent covers the property both operators and the
// compensation path depend on.
//
// Revocation is retried: by a person working through a failure, and by
// compensateFailedProvisioning. A second attempt that failed because there was nothing left to
// delete would make the retry look like a new problem.
func TestRevokeSubscriberPrincipal_IsIdempotent(t *testing.T) {
	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)
	principal := strings.TrimPrefix(testSubscriberPrincipal(t), kafkaPrincipalPrefix)

	fake.scram[principal] = []kafka.ScramMechanism{kafka.ScramMechanismSha512}

	require.NoError(t, admin.RevokeSubscriberPrincipal(context.Background(), principal))
	assert.Empty(t, fake.scram)

	require.NoError(t, admin.RevokeSubscriberPrincipal(context.Background(), principal),
		"deleting a credential that no longer exists is the desired end state, not an error")
}

// TestRevokeSubscriberPrincipal_ReportsARealFailure keeps idempotence from swallowing
// everything.
//
// RESOURCE_NOT_FOUND is success; any other broker error is a credential that is still live and
// must be reported, because the caller is about to delete the registry row that names it.
func TestRevokeSubscriberPrincipal_ReportsARealFailure(t *testing.T) {
	fake := newFakeAdminClient()
	fake.scramDeleteError = errors.New("not authorized")
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	err := admin.RevokeSubscriberPrincipal(context.Background(), "blnk-sub-sub_0f6e2c8a")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not authorized")
}

// TestRevokeSubscriber_RefusesWithoutASubscriber covers the guard that keeps a nil row from
// becoming a broad deletion.
func TestRevokeSubscriber_RefusesWithoutASubscriber(t *testing.T) {
	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	require.Error(t, admin.RevokeSubscriber(context.Background(), nil))
	assert.Zero(t, fake.totalCalls(), "nothing may be deleted without a row saying what to delete")
}

// ---------------------------------------------------------------------------------------
// Zero-loss reconciliation — OBS-01
// ---------------------------------------------------------------------------------------

// TestReconcileAgainstOutbox_TreatsMessagesAsALowerBoundOnEvents is the OBS-01 guard.
//
// Summed end offsets count RECORDS and the outbox counts EVENTS, and the reconciliation used to
// be documented as an equality between them. It cannot hold: a redelivery after a crash writes
// a second record for one event, a replay writes another on purpose, and a dead-lettered event
// has a record on its `.dlt` topic. So messages >= events always, and an equality check reports
// loss on a healthy system the first time anything is redelivered — the alert that gets muted,
// taking the real signal with it.
func TestReconcileAgainstOutbox_TreatsMessagesAsALowerBoundOnEvents(t *testing.T) {
	t.Run("a surplus is expected, not loss", func(t *testing.T) {
		report := TopicOffsetReport{EndOffsetSum: 1_100, RetainedCount: 1_100, MeasuredAt: time.Now().UTC()}

		verdict := ReconcileAgainstOutbox(report, 1_000)
		assert.False(t, verdict.LossDetected,
			"100 more records than events is redelivery and replay overhead, which is normal")
		assert.True(t, verdict.Conclusive)
		assert.Equal(t, int64(100), verdict.Overhead)
		assert.Contains(t, verdict.Summary(), "NO LOSS DETECTED")
		assert.Contains(t, verdict.Summary(), "audit consumer",
			"the verdict must not be mistaken for a proof that every event is present")
	})

	t.Run("a shortfall is loss", func(t *testing.T) {
		report := TopicOffsetReport{EndOffsetSum: 990, RetainedCount: 990, MeasuredAt: time.Now().UTC()}

		verdict := ReconcileAgainstOutbox(report, 1_000)
		assert.True(t, verdict.LossDetected,
			"fewer records than rows means rows claim a publication that never happened")
		assert.Equal(t, int64(-10), verdict.Overhead,
			"the shortfall must stay signed; clamping it would erase the only signal here")
		assert.Contains(t, verdict.Summary(), "LOSS DETECTED")
	})

	t.Run("exact equality is not loss", func(t *testing.T) {
		report := TopicOffsetReport{EndOffsetSum: 1_000, RetainedCount: 1_000, MeasuredAt: time.Now().UTC()}

		verdict := ReconcileAgainstOutbox(report, 1_000)
		assert.False(t, verdict.LossDetected, "the boundary is inclusive: equal is not short")
		assert.Zero(t, verdict.Overhead)
	})
}

// TestReconcileAgainstOutbox_RefusesToConcludeFromAnIncompleteMeasurement covers the caveats,
// which are what stop a green verdict being reported from a number that was never complete.
func TestReconcileAgainstOutbox_RefusesToConcludeFromAnIncompleteMeasurement(t *testing.T) {
	cases := map[string]TopicOffsetReport{
		"a missing topic": {
			EndOffsetSum: 1_000, RetainedCount: 1_000,
			MissingTopics: []string{"blnk.identities"},
		},
		"an unavailable partition": {
			EndOffsetSum: 1_000, RetainedCount: 1_000,
			PartitionsUnavailable: 2,
		},
		"retention has deleted records": {
			EndOffsetSum: 1_000, RetainedCount: 400,
		},
	}

	for name, report := range cases {
		t.Run(name, func(t *testing.T) {
			verdict := ReconcileAgainstOutbox(report, 1_000)
			assert.False(t, verdict.Conclusive, "%s makes the count incomplete", name)
			require.NotEmpty(t, verdict.Caveats, "the reason must be stated, not just flagged")
			assert.Contains(t, verdict.Summary(), "INCONCLUSIVE")
		})
	}

	t.Run("loss is still reported on an inconclusive measurement", func(t *testing.T) {
		// A shortfall is unambiguous even when the count is incomplete: an incomplete count
		// can only ever be LOWER than the truth, so it cannot manufacture a shortfall.
		verdict := ReconcileAgainstOutbox(TopicOffsetReport{
			EndOffsetSum: 900, RetainedCount: 900, PartitionsUnavailable: 1,
		}, 1_000)

		assert.True(t, verdict.LossDetected)
		assert.False(t, verdict.Conclusive)
		assert.Contains(t, verdict.Summary(), "LOSS DETECTED",
			"loss takes precedence over inconclusiveness in the summary")
	})
}

// TestAdminResultTypes_AreNotResponseShapes pins the service boundary this file's results
// and reports sit behind.
//
// Two shapes for one endpoint is the failure this prevents. api/model owns the API
// contract: KafkaCredentialsResponse is the single authoritative body for a credential
// issuance and EventOutboxStatsResponse for the statistics endpoint, and both are what the
// handler tests assert over real HTTP. If an admin result were serialised directly instead,
// a client would receive different key names for the same facts — "principal" where the
// contract says "username", "topics" where it says "authorized_topics",
// "consumer_group_prefix" where it says "consumer_group_id" — and the two shapes would
// drift apart with only one of them tested.
//
// The first assertion is structural: no admin result or report declares a json tag, so
// nothing about these types suggests they are meant for an encoder. The second is the
// mapping itself, written out as the handler must write it, followed by proof that the
// facts a subscriber has no business knowing — the PBKDF2 iteration count, how many ACL
// bindings were written, whether an existing credential was just invalidated, whether the
// broker's authorizer is enforcing at all, and whether a failed provisioning had to be
// compensated — reach no client. Those describe Blnk's own isolation model, and disclosing
// them hands a map of it to anybody holding an API key.
func TestAdminResultTypes_AreNotResponseShapes(t *testing.T) {
	internalValues := []interface{}{
		SubscriberProvisioningResult{},
		SubscriberProvisioningRequest{},
		TopicAssurance{},
		TopicAssuranceReport{},
		ConsumerLagReport{},
		TopicLag{},
		PartitionLag{},
		TopicOffsetReport{},
		TopicOffsetSnapshot{},
		PartitionOffsetSnapshot{},
		OutboxReconciliation{},
	}

	for _, value := range internalValues {
		valueType := reflect.TypeOf(value)

		for index := 0; index < valueType.NumField(); index++ {
			field := valueType.Field(index)

			assert.Empty(t, field.Tag.Get("json"),
				"%s.%s carries a json tag; admin results and reports are internal values, and a "+
					"tagged field invites being encoded straight into a response beside the "+
					"api/model DTO that owns that contract",
				valueType.Name(), field.Name)
		}
	}

	// The mapping the credential endpoint must perform: every field of the response comes
	// either from configuration, from the generated secret, or from the named fields of the
	// provisioning result. Nothing else crosses.
	result := SubscriberProvisioningResult{
		SubscriberID:        "sub_reconciliation",
		Principal:           "blnk-sub-sub_reconciliation",
		Mechanism:           SubscriberSASLMechanism,
		Iterations:          DefaultScramIterations,
		Topics:              []string{"blnk.transactions", "blnk.balances"},
		ConsumerGroupPrefix: "blnk-sub-sub_reconciliation.",
		ACLBindings:         5,
		CredentialReplaced:  true,
		AuthorizerActive:    true,
		CredentialWritten:   true,
		ProvisionedAt:       time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC),
	}

	response := apimodel.KafkaCredentialsResponse{
		Brokers:          []string{"kafka-0:9092", "kafka-1:9092"},
		BrokerEndpoint:   "kafka-0:9092,kafka-1:9092",
		AuthorizedTopics: result.Topics,
		ConsumerGroupID:  result.ConsumerGroupPrefix,
		Username:         result.Principal,
		Password:         sentinelPassword,
		Mechanism:        result.Mechanism,
		IssuedAt:         result.ProvisionedAt,
	}

	encoded, err := json.Marshal(response)
	require.NoError(t, err, "the credential response must serialise")

	var body map[string]interface{}
	require.NoError(t, json.Unmarshal(encoded, &body))

	// The contract's own keys, so a mapping that silently dropped one fails here.
	for _, key := range []string{
		"brokers", "broker_endpoint", "authorized_topics", "consumer_group_id",
		"username", "password", "mechanism", "issued_at",
	} {
		assert.Contains(t, body, key, "the credential contract requires %q", key)
	}

	assert.Equal(t, result.Principal, body["username"],
		"the principal is reported under the contract's name for it, not the admin layer's")
	assert.Equal(t, result.ConsumerGroupPrefix, body["consumer_group_id"])
	assert.Equal(t, []interface{}{"blnk.transactions", "blnk.balances"}, body["authorized_topics"])

	// The internal-only facts, named individually so that adding one to the DTO later fails
	// this test rather than shipping.
	for _, key := range []string{
		"principal", "topics", "consumer_group_prefix", "subscriber_id",
		"iterations", "acl_bindings", "credential_replaced", "authorizer_active",
		"credential_written", "compensated", "provisioned_at",
	} {
		assert.NotContains(t, body, key,
			"%q is an admin-layer field and must not appear in the credential response", key)
	}

	rendered := string(encoded)
	assert.NotContains(t, rendered, strconv.Itoa(DefaultScramIterations),
		"the PBKDF2 iteration count must not be disclosed to a subscriber")
	assert.NotContains(t, rendered, "authorizer",
		"whether the broker enforces ACLs is Blnk's security posture, not a subscriber's business")
}

// TestConsumerLag_ReusesOnePartitionAndOffsetSnapshotAcrossGroups is the request fan-out
// reduction on the metrics path.
//
// A lag sweep asks three questions per subscriber, and only one of them — the group's
// committed offsets — actually differs between subscribers. The partition layout and the end
// offsets are properties of the topics, identical for every group reading them, so asking
// them per subscriber made an N-subscriber sweep cost 3N round trips where 1 + 1 + N would do.
//
// The assertion is on the round-trip counts, because the reports are identical either way.
func TestConsumerLag_ReusesOnePartitionAndOffsetSnapshotAcrossGroups(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	// The fake serves one set of committed offsets for any group, which is exactly right
	// here: the test is about how many round trips each group costs, not about their
	// individual positions.
	fake.withOffsets("blnk.transactions", 0, 0, 100).withCommitted("blnk.transactions", 0, 40)

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)
	current := time.Now()
	admin.now = func() time.Time { return current }

	const groups = 3
	for _, group := range []string{"group-a", "group-b", "group-c"} {
		report, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{
			SubscriberID: group,
			GroupID:      group,
			Topics:       []string{"blnk.transactions"},
		})
		require.NoError(t, err)
		assert.Equal(t, int64(60), report.TotalLag,
			"a cached snapshot must not change the answer; %s", group)
	}

	assert.Equal(t, 1, fake.callCount("Metadata"),
		"the partition layout is a property of the topics, so ONE read serves every group in the sweep")
	assert.Equal(t, 1, fake.callCount("ListOffsets"),
		"the end offsets are a property of the topics too; asking per subscriber is the 3N fan-out this removes")
	assert.Equal(t, groups, fake.callCount("OffsetFetch"),
		"only the committed offsets differ per group, so this is the one read that must happen per subscriber")

	// Past the TTL the shared reads happen again, so the gauge cannot serve a stale head
	// indefinitely.
	current = current.Add(defaultOffsetSnapshotTTL + time.Second)

	_, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{
		GroupID: "group-a",
		Topics:  []string{"blnk.transactions"},
	})
	require.NoError(t, err)

	assert.Equal(t, 2, fake.callCount("Metadata"),
		"the snapshot must expire; a permanently cached end offset would understate lag forever")
	assert.Equal(t, 2, fake.callCount("ListOffsets"))
}

// TestInvalidateOffsetSnapshot_ForcesTheNextReadToTheBroker covers the case where a cached
// answer is known to be wrong rather than merely old.
//
// Creating or repartitioning topics changes the partition layout a snapshot describes, so a
// caller that has just done either must be able to discard the snapshot instead of waiting
// out the TTL with a layout it knows is stale. EnsureTopics does exactly that whenever it
// created or grew anything.
func TestInvalidateOffsetSnapshot_ForcesTheNextReadToTheBroker(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	fake.withOffsets("blnk.transactions", 0, 0, 10).withCommitted("blnk.transactions", 0, 4)

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	request := ConsumerLagRequest{GroupID: "acme-recon-group", Topics: []string{"blnk.transactions"}}

	_, err := admin.ConsumerLag(context.Background(), request)
	require.NoError(t, err)
	_, err = admin.ConsumerLag(context.Background(), request)
	require.NoError(t, err)
	assert.Equal(t, 1, fake.callCount("Metadata"), "the second read comes from the snapshot")

	admin.InvalidateOffsetSnapshot()

	_, err = admin.ConsumerLag(context.Background(), request)
	require.NoError(t, err)
	assert.Equal(t, 2, fake.callCount("Metadata"),
		"invalidation must force the next read to the broker, so a caller that just repartitioned is not served a stale layout")
}

// TestWithOffsetSnapshotTTL_CanDisableCachingEntirely keeps the caching optional for a caller
// that must observe raw round trips.
//
// A negative TTL disables it. That is deliberately not reachable from configuration: the
// default exists so PRODUCTION GETS THE BENEFIT WITHOUT OPTING IN, and an operator has no
// reason to turn coalescing off — but a test asserting on exact round-trip counts does.
func TestWithOffsetSnapshotTTL_CanDisableCachingEntirely(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	fake.withOffsets("blnk.transactions", 0, 0, 10).withCommitted("blnk.transactions", 0, 4)

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1).WithOffsetSnapshotTTL(-1)

	request := ConsumerLagRequest{GroupID: "acme-recon-group", Topics: []string{"blnk.transactions"}}
	for i := 0; i < 3; i++ {
		_, err := admin.ConsumerLag(context.Background(), request)
		require.NoError(t, err)
	}

	assert.Equal(t, 3, fake.callCount("Metadata"), "a negative TTL disables the snapshot cache")

	// And zero restores the default rather than disabling it, so a caller cannot switch
	// caching off by accident.
	assert.Equal(t, defaultOffsetSnapshotTTL,
		newTestKafkaAdmin(newFakeAdminClient(), MinTopicPartitions, 1).snapshotTTL(),
		"a client that was never configured must still cache; the benefit must not require opting in")
	assert.Equal(t, defaultOffsetSnapshotTTL,
		newTestKafkaAdmin(newFakeAdminClient(), MinTopicPartitions, 1).WithOffsetSnapshotTTL(0).snapshotTTL(),
		"zero restores the default")
}

// TestPartitionOffsetSnapshot_NeverCachesAFailedRead is the correctness half of the cache.
//
// A transient broker problem must be re-asked on the next call. Caching it would keep
// answering a failure that had already cleared for the life of the TTL, and the lag gauge
// would stay wrong instead of self-correcting on the next sweep.
func TestPartitionOffsetSnapshot_NeverCachesAFailedRead(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	fake.transportErrors["Metadata"] = errors.New("broker unreachable")

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	request := ConsumerLagRequest{GroupID: "acme-recon-group", Topics: []string{"blnk.transactions"}}

	_, first := admin.ConsumerLag(context.Background(), request)
	require.Error(t, first)
	_, second := admin.ConsumerLag(context.Background(), request)
	require.Error(t, second)

	assert.Equal(t, 2, fake.callCount("Metadata"),
		"a failed read must not be remembered: the next sweep has to see the broker recover")

	// And once it does recover, the very next call succeeds rather than serving the
	// remembered failure.
	delete(fake.transportErrors, "Metadata")
	fake.withOffsets("blnk.transactions", 0, 0, 10).withCommitted("blnk.transactions", 0, 4)

	report, err := admin.ConsumerLag(context.Background(), request)
	require.NoError(t, err)
	assert.Equal(t, int64(6), report.TotalLag)
}

// TestProvisionSubscriberPrincipal_MemoisesTheAuthorizerProbe covers the probe on the
// provisioning path.
//
// Whether the broker enforces ACLs comes from its own startup configuration, so it cannot
// change while it is up. Asking on every issuance re-learned a fixed fact using a serial
// DescribeACLs round trip out of the five-second budget R-7 sets.
//
// The memo must not be permanent, though: a long-lived server can be pointed at a cluster
// restarted with different settings, and continuing to report "enforcing" would be reporting
// a security property that has stopped being true. So expiry is asserted too, through the
// injected clock rather than by sleeping.
func TestProvisionSubscriberPrincipal_MemoisesTheAuthorizerProbe(t *testing.T) {
	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	current := time.Now()
	admin.now = func() time.Time { return current }

	for i := 0; i < 4; i++ {
		_, err := admin.ProvisionSubscriberPrincipal(context.Background(),
			NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword))
		require.NoError(t, err)
	}

	assert.Equal(t, 1, fake.callCount("DescribeACLs"),
		"the authorizer probe must be answered from the memo after the first ask; it re-learns a fact fixed at broker startup")

	// Past the TTL the question is asked again, so a cluster restarted with a different
	// authorizer setting is noticed rather than reported from a stale answer.
	current = current.Add(defaultOffsetSnapshotTTL + time.Second)

	_, err := admin.ProvisionSubscriberPrincipal(context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword))
	require.NoError(t, err)

	assert.Equal(t, 2, fake.callCount("DescribeACLs"),
		"the memo must expire; a permanent one would keep reporting an isolation guarantee that may have stopped holding")
}

// TestAuthorizerProbeMemo_IsNotConsultedAfterAFailure keeps the FAIL-CLOSED guarantee intact
// across the memo.
//
// The probe is what stands between a subscriber credential and a broker that would accept
// every ACL and enforce none. A memo that remembered a FAILED probe would answer "could not
// confirm" for the life of the TTL — but far worse, a memo that remembered a failure as
// "inactive" or a success it never got as "active" would hand out a credential against an
// unverified boundary. Only a definite answer is ever remembered.
func TestAuthorizerProbeMemo_IsNotConsultedAfterAFailure(t *testing.T) {
	fake := newFakeAdminClient()
	fake.transportErrors["DescribeACLs"] = errors.New("broker unreachable")

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	_, first := admin.ProvisionSubscriberPrincipal(context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword))
	require.ErrorIs(t, first, ErrAuthorizerNotEnforcing,
		"an unanswerable probe must still fail closed")
	_, second := admin.ProvisionSubscriberPrincipal(context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword))
	require.ErrorIs(t, second, ErrAuthorizerNotEnforcing)

	assert.Equal(t, 2, fake.callCount("DescribeACLs"),
		"a failed probe must be re-asked, not remembered: caching it would turn one bad answer into TTL-long silence about whether the isolation guarantee holds")
	assert.Zero(t, fake.callCount("AlterUserScramCredentials"),
		"no credential may be written while the boundary is unverified")
}

// TestSubscriberSecret_CannotBeLeakedByAnyRenderingPath is the CWE-532 boundary around the
// only secret this package handles.
//
// # Why the type exists rather than a convention
//
// The password used to be a plain string field. Nothing in the file leaked it — that is
// what TestAdminResultTypes_HaveNowhereToPutTheSecret asserts — but the protection was a
// property of the code that happened to exist rather than of the value itself. A plain
// string is carried into a log the moment anyone writes logrus.WithField("request", req),
// into a test failure message whenever %+v is used on anything containing it, and into a
// response body the moment the struct is embedded in one. Each of those is one ordinary
// line away, none of them fails, and the leak is permanent because logs are retained.
//
// So every rendering path is asserted, not just the obvious one. fmt consults Formatter
// FIRST and ignores Stringer entirely when it is present, which is why one Format method
// covers verbs — %+v, %#v, %q, %x — that a Stringer alone would not reach.
func TestSubscriberSecret_CannotBeLeakedByAnyRenderingPath(t *testing.T) {
	secret := NewSubscriberSecret(sentinelPassword)

	t.Run("every fmt verb renders the placeholder", func(t *testing.T) {
		for _, verb := range []string{"%s", "%v", "%+v", "%#v", "%q", "%x", "%d"} {
			rendered := fmt.Sprintf(verb, secret)
			assert.NotContains(t, rendered, sentinelPassword,
				"%s must not render the plaintext, got %q", verb, rendered)
			assert.Contains(t, rendered, RedactedSecretPlaceholder,
				"%s must render the placeholder so an absence and a redaction are distinguishable", verb)
		}
	})

	t.Run("json and text marshalling render the placeholder", func(t *testing.T) {
		encoded, err := json.Marshal(secret)
		require.NoError(t, err,
			"marshalling must SUCCEED rather than error: an error would make every struct carrying a secret unserialisable, and a caller would work around it by copying the field out")
		assert.NotContains(t, string(encoded), sentinelPassword)
		assert.JSONEq(t, `"`+RedactedSecretPlaceholder+`"`, string(encoded))

		text, err := secret.MarshalText()
		require.NoError(t, err)
		assert.Equal(t, RedactedSecretPlaceholder, string(text))
	})

	t.Run("the enclosing request cannot leak it either", func(t *testing.T) {
		// The realistic leak: the whole request rendered, not the field on its own.
		request := NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword)

		for _, verb := range []string{"%v", "%+v", "%#v"} {
			rendered := fmt.Sprintf(verb, request)
			assert.NotContains(t, rendered, sentinelPassword,
				"%s of the whole request must not expose the password, got %q", verb, rendered)
		}

		encoded, err := json.Marshal(request)
		require.NoError(t, err)
		assert.NotContains(t, string(encoded), sentinelPassword,
			"serialising the request must not expose the password")

		// And through a logrus entry, which is the path the original defect took.
		logger, hook := logtest.NewNullLogger()
		logger.WithField("request", request).Info("provisioning")
		require.Len(t, hook.Entries, 1)
		assert.NotContains(t, fmt.Sprint(hook.Entries[0].Data["request"]), sentinelPassword,
			"a logrus field carrying the request must not expose the password")
	})

	t.Run("what is observable is enough to work with", func(t *testing.T) {
		// The length is safe to publish and is the one property worth reporting: it lets a
		// test or an operator confirm a secret of the expected strength was generated
		// without the value appearing anywhere.
		assert.Equal(t, len(sentinelPassword), secret.Len())
		assert.False(t, secret.IsZero())

		var absent SubscriberSecret
		assert.True(t, absent.IsZero(), "the zero value must read as 'no secret'")
		assert.Zero(t, absent.Len())
		assert.Contains(t, fmt.Sprint(absent), RedactedSecretPlaceholder,
			"even an empty secret renders the placeholder, so a leak and an absence never look the same")
	})

	t.Run("the field is exported, which is what makes the redaction work", func(t *testing.T) {
		// fmt can only call a field's methods when it can take its interface, so an
		// UNEXPORTED field of a redacting type would be printed by %+v as its raw
		// contents. Hiding the field would defeat the redaction rather than strengthen it,
		// which is the opposite of what it looks like.
		field, ok := reflect.TypeOf(SubscriberProvisioningRequest{}).FieldByName("Password")
		require.True(t, ok, "the request must declare Password")
		assert.True(t, field.IsExported(),
			"Password must stay exported or fmt cannot call its redacting methods")
		assert.Equal(t, reflect.TypeOf(SubscriberSecret{}), field.Type,
			"Password must be the redacting type, not a plain string")
	})
}
