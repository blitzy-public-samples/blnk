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
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
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

	apimodel "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
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
//
// The longhand inventory is then tied back to event_topics.go — the one place topic names
// are composed — by TestEventTopicInventory_MatchesTheSingleSourceOfTruth. That is the
// bridge that lets both properties hold at once: the literal list keeps every assertion
// here non-vacuous, and the bridge stops the literal list from drifting away from the
// names the pipeline actually publishes to.

// expectedEventTopics is the topic inventory, spelled out independently of
// event_topics.go: the four category topics followed by their four dead-letter siblings,
// in the canonical order provisioning uses.
//
// blnk.system carries ledger.created and system.error and is also where an event type the
// catalogue does not recognise is routed, so it must be provisioned with the same geometry as
// every other topic. A system topic that does not exist would strand both Blnk's own records
// and exactly the events that already indicate a routing defect.
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
	// replicas maps a topic to the size of each partition's replica set. An absent entry
	// models a broker that reported no replica information, which the assurance pass treats
	// as "not visible yet" rather than as under-replication.
	replicas map[string]int
	// first and end are the offset bounds per topic and partition.
	first map[string]map[int]int64
	end   map[string]map[int]int64
	// committed is the consumer group's committed offsets per topic and partition.
	committed map[string]map[int]int64

	// windowStart is the offset a TIMESTAMP request resolves to, per partition.
	//
	// A timestamp request is a DIFFERENT question from FirstOffsetOf or LastOffsetOf and
	// kafka-go answers it in a different field — the Offsets map rather than FirstOffset or
	// LastOffset — so it is modelled separately here (PERF-P05). A partition with no entry
	// resolves to -1, which is the broker's "nothing that recent". A partition listed in
	// windowStartOmitted is answered with NO entry at all, which is what an unreadable
	// partition looks like and must stay distinguishable from an empty one.
	windowStart        map[string]map[int]int64
	windowStartOmitted map[string]map[int]bool

	// timeOffsetRequests counts the timestamp requests, so a test can assert that a window is
	// read in its OWN round trip and only when one was asked for.
	timeOffsetRequests int

	// records is the fake log: the records Fetch serves, per topic and partition, in offset
	// order. Each record carries the offset it sits at, so a test can model a compacted or
	// sparse log without the fake inventing offsets.
	records map[string]map[int][]kafka.Record

	// fetchErrors is the error Fetch reports INSIDE its response for a topic and partition,
	// which is where the authorizer's verdict arrives. A test uses it to model
	// TOPIC_AUTHORIZATION_FAILED without a broker.
	fetchErrors map[string]map[int]error

	// fetchRequests records what the gateway asked the broker for, so the bounds it applies
	// are assertable rather than inferred from what came back.
	fetchRequests []*kafka.FetchRequest
	// scram maps a principal to the SCRAM mechanisms it holds credentials for.
	scram map[string][]kafka.ScramMechanism
	// bindings is the broker's ACL store: CreateACLs adds to it, DeleteACLs removes the
	// filters' matches from it, and DescribeACLs answers out of it. Modelling the store
	// rather than answering a constant is what makes RECONCILIATION observable — a fake
	// that always reports "no bindings" can never show a surplus being removed, which is
	// the entire property AUTH-02 adds.
	bindings []kafka.ACLEntry
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

	// --- Injected behaviour ---

	// onCall, when set, runs as each call is recorded and BEFORE the call's context is
	// inspected or any injected failure is returned.
	//
	// It exists to disturb the world at a precise point INSIDE a multi-step operation. The
	// fake is synchronous, so a test cannot otherwise place an event — a caller going away, a
	// concurrent provisioner — between the credential write and the ACL batch, which is the
	// only window in which the compensation path can be reached with a dead caller. Cancelling
	// from here is what makes CLEAN-01 observable rather than assumed.
	//
	// IT RUNS WITH THE FAKE'S MUTEX HELD, so it must not call back into the fake. Cancelling a
	// context, closing a channel, reading a captured local and calling t.Log are all safe;
	// f.callCount and f.heldBindings would deadlock.
	onCall func(method string)

	// --- Recordings ---

	calls                    []string
	contexts                 []fakeAdminContextObservation
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
		windowStart:         map[string]map[int]int64{},
		windowStartOmitted:  map[string]map[int]bool{},
		records:             map[string]map[int][]kafka.Record{},
		fetchErrors:         map[string]map[int]error{},
	}
}

// withRecords seeds the fake log of one partition, numbering offsets from the given base.
//
// The key and value are the two fields the gateway acts on: the key decides whether a
// key-scoped subscriber is entitled to the record, and the value is what a delivered record
// carries. kafka.NewBytes is used rather than a raw slice because RecordReader consumers read
// through the Bytes interface, exactly as the real client's do.
func (f *fakeAdminClient) withRecords(
	topic string,
	partition int,
	base int64,
	pairs ...[2]string,
) *fakeAdminClient {
	if f.records[topic] == nil {
		f.records[topic] = map[int][]kafka.Record{}
	}

	stamp := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	for index, pair := range pairs {
		f.records[topic][partition] = append(f.records[topic][partition], kafka.Record{
			Offset: base + int64(index),
			Time:   stamp.Add(time.Duration(index) * time.Second),
			Key:    kafka.NewBytes([]byte(pair[0])),
			Value:  kafka.NewBytes([]byte(pair[1])),
		})
	}

	return f
}

// withFetchError makes Fetch answer one partition with a broker-side error, which is how an
// authorization refusal reaches a caller.
func (f *fakeAdminClient) withFetchError(topic string, partition int, err error) *fakeAdminClient {
	if f.fetchErrors[topic] == nil {
		f.fetchErrors[topic] = map[int]error{}
	}
	f.fetchErrors[topic][partition] = err

	return f
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

// withWindowStart sets the offset a TIMESTAMP request resolves to for one partition: the first
// record written at or after the reconciliation window's start.
func (f *fakeAdminClient) withWindowStart(topic string, partition int, offset int64) *fakeAdminClient {
	if f.windowStart[topic] == nil {
		f.windowStart[topic] = map[int]int64{}
	}
	f.windowStart[topic][partition] = offset

	return f
}

// withUnreadableWindow makes one partition answer a timestamp request with NO offset at all.
//
// That is what an unreadable partition looks like, and it must stay distinguishable from a
// partition holding nothing that recent: reading the first as the second makes the broker-side
// count short, and short is indistinguishable from loss.
func (f *fakeAdminClient) withUnreadableWindow(topic string, partition int) *fakeAdminClient {
	if f.windowStartOmitted[topic] == nil {
		f.windowStartOmitted[topic] = map[int]bool{}
	}
	f.windowStartOmitted[topic][partition] = true

	return f
}

// snapshotTimeOffsetRequests returns how many timestamp requests were issued.
func (f *fakeAdminClient) snapshotTimeOffsetRequests() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.timeOffsetRequests
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

// fakeAdminContextObservation is what one call saw of its context.
//
// A bare "did it have a deadline" boolean cannot tell a cleanup that ran on a LIVE detached
// context apart from one that inherited an already-cancelled caller and returned instantly, and
// that distinction IS the CLEAN-01 guarantee: the compensation exists for the case where the
// caller's budget has expired, so a compensation that merely inherits the expiry does nothing at
// all while reporting that it tried. Both the error and the deadline are therefore kept, per
// call, in call order.
type fakeAdminContextObservation struct {
	// method is the admin call this observation belongs to.
	method string
	// err is ctx.Err() at the instant the call was made: nil for a live context.
	err error
	// hasDeadline and deadline are the bound the call ran under. A detached compensation must
	// still be BOUNDED, so "no deadline at all" is as much a failure as an expired one.
	hasDeadline bool
	deadline    time.Time
}

// record notes a call, what its context looked like, and returns any injected transport failure
// for that method.
func (f *fakeAdminClient) record(method string, ctx context.Context) error {
	f.calls = append(f.calls, method)

	deadline, hasDeadline := ctx.Deadline()

	// The hook runs BEFORE the context is inspected, so an event it triggers — a cancellation
	// above all — is visible to this very call rather than only to the next one. That ordering
	// is what lets a test place the caller's departure inside the round trip that fails, which
	// is the window the compensation path is reached from.
	if f.onCall != nil {
		f.onCall(method)
	}

	contextErr := ctx.Err()
	f.contexts = append(f.contexts, fakeAdminContextObservation{
		method:      method,
		err:         contextErr,
		hasDeadline: hasDeadline,
		deadline:    deadline,
	})

	// A cancelled or expired context is refused, exactly as a real round trip would refuse
	// it. Without this the fake is BLIND to cancellation, and the guarantees that exist
	// specifically to survive it — the detached compensation context, the detached relay
	// bookkeeping — cannot be told apart from code that simply passes the caller's context
	// through. That is the difference between a test that proves the guarantee and one that
	// would pass either way.
	if contextErr != nil {
		return contextErr
	}

	return f.transportErrors[method]
}

// observedContextsFor returns, in call order, what every call to one method saw of its context.
func (f *fakeAdminClient) observedContextsFor(method string) []fakeAdminContextObservation {
	f.mu.Lock()
	defer f.mu.Unlock()

	observations := make([]fakeAdminContextObservation, 0, len(f.contexts))
	for _, observation := range f.contexts {
		if observation.method == method {
			observations = append(observations, observation)
		}
	}

	return observations
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

	for _, observation := range f.contexts {
		if !observation.hasDeadline {
			return false
		}
	}

	return len(f.contexts) > 0
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

	// A successful creation is retained, so a later DescribeACLs reports it. Kafka accepts a
	// binding that already exists without creating a second one, so the store is a set.
	for _, entry := range req.ACLs {
		if !f.holdsBinding(entry) {
			f.bindings = append(f.bindings, entry)
		}
	}

	return &kafka.CreateACLsResponse{Errors: make([]error, len(req.ACLs))}, nil
}

// holdsBinding reports whether the modelled store already contains a binding. Callers must
// hold the mutex.
func (f *fakeAdminClient) holdsBinding(entry kafka.ACLEntry) bool {
	for _, held := range f.bindings {
		if fakeACLKey(held) == fakeACLKey(entry) {
			return true
		}
	}

	return false
}

// withBinding seeds the modelled ACL store directly, so a test can start from a broker that
// already holds a grant nothing in the test created — which is how a STALE binding from a
// previous authorization is expressed.
func (f *fakeAdminClient) withBinding(entry kafka.ACLEntry) *fakeAdminClient {
	f.bindings = append(f.bindings, entry)

	return f
}

// authorizerProbeCount counts only the DescribeACLs requests that are the ENFORCEMENT PROBE.
//
// Two different callers use DescribeACLs now, and a raw call count conflates them: the probe
// asks a cluster-wide question with no principal filter, while ACL reconciliation asks a
// principal-scoped one. Counting the probe by its own shape is what keeps the memo assertion
// measuring the memo rather than the number of provisionings.
func (f *fakeAdminClient) authorizerProbeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	probes := 0
	for _, req := range f.describeACLsRequests {
		if req.Filter.PrincipalFilter == "" {
			probes++
		}
	}

	return probes
}

// heldBindings returns a copy of the modelled ACL store.
func (f *fakeAdminClient) heldBindings() []kafka.ACLEntry {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]kafka.ACLEntry(nil), f.bindings...)
}

// fakeACLKey renders a binding as its identifying tuple. It is deliberately written out here
// rather than delegating to the production aclBindingKey, so that a change to production's own
// notion of binding identity cannot make the fake agree with it by construction.
func fakeACLKey(entry kafka.ACLEntry) string {
	return fmt.Sprintf("%d/%s/%d/%s/%s/%d/%d",
		entry.ResourceType, entry.ResourceName, entry.ResourcePatternType,
		entry.Principal, entry.Host, entry.Operation, entry.PermissionType)
}

// fakeACLFilterMatches applies one DeleteACLs filter to one held binding, following Kafka's own
// rule that an empty name, principal or host filter and an Any type filter match anything.
func fakeACLFilterMatches(filter kafka.DeleteACLsFilter, held kafka.ACLEntry) bool {
	if filter.ResourceTypeFilter != kafka.ResourceTypeAny && filter.ResourceTypeFilter != held.ResourceType {
		return false
	}

	if filter.ResourceNameFilter != "" && filter.ResourceNameFilter != held.ResourceName {
		return false
	}

	if filter.ResourcePatternTypeFilter != kafka.PatternTypeAny &&
		filter.ResourcePatternTypeFilter != held.ResourcePatternType {
		return false
	}

	if filter.PrincipalFilter != "" && filter.PrincipalFilter != held.Principal {
		return false
	}

	if filter.HostFilter != "" && filter.HostFilter != held.Host {
		return false
	}

	if filter.Operation != kafka.ACLOperationTypeAny && filter.Operation != held.Operation {
		return false
	}

	return filter.PermissionType == kafka.ACLPermissionTypeAny ||
		filter.PermissionType == held.PermissionType
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

			// A filter that succeeded removes what it matched from the modelled store. A
			// filter whose result carries an error removes nothing, which is what makes a
			// partially failed deletion observable as a binding still standing.
			retained := make([]kafka.ACLEntry, 0, len(f.bindings))
			for _, held := range f.bindings {
				if !fakeACLFilterMatches(filter, held) {
					retained = append(retained, held)
				}
			}
			f.bindings = retained
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

	// Recorded BEFORE the injected transport failure, so the recording describes every
	// ATTEMPT rather than only the answered ones. authorizerProbeCount reads this slice to
	// tell the enforcement probe apart from a reconciliation read, and a probe that failed is
	// exactly the one whose re-asking has to be provable.
	f.describeACLsRequests = append(f.describeACLsRequests, req)

	if err := f.record("DescribeACLs", ctx); err != nil {
		return nil, err
	}

	if f.securityDisabled {
		return &kafka.DescribeACLsResponse{Error: kafka.SecurityDisabled}, nil
	}

	// The modelled store is grouped by resource exactly as the broker groups it, so the
	// production reader has to walk resources and their nested ACL descriptions rather than a
	// flat list — the shape it would get wrong against a real broker.
	byResource := make(map[string]*kafka.ACLResource)
	order := make([]string, 0, len(f.bindings))

	for _, held := range f.bindings {
		if !fakeACLFilterMatches(kafka.DeleteACLsFilter{
			ResourceTypeFilter:        req.Filter.ResourceTypeFilter,
			ResourceNameFilter:        req.Filter.ResourceNameFilter,
			ResourcePatternTypeFilter: req.Filter.ResourcePatternTypeFilter,
			PrincipalFilter:           req.Filter.PrincipalFilter,
			HostFilter:                req.Filter.HostFilter,
			Operation:                 req.Filter.Operation,
			PermissionType:            req.Filter.PermissionType,
		}, held) {
			continue
		}

		key := fmt.Sprintf("%d/%s/%d", held.ResourceType, held.ResourceName, held.ResourcePatternType)
		if _, seen := byResource[key]; !seen {
			byResource[key] = &kafka.ACLResource{
				ResourceType: held.ResourceType,
				ResourceName: held.ResourceName,
				PatternType:  held.ResourcePatternType,
			}
			order = append(order, key)
		}

		byResource[key].ACLs = append(byResource[key].ACLs, kafka.ACLDescription{
			Principal:      held.Principal,
			Host:           held.Host,
			Operation:      held.Operation,
			PermissionType: held.PermissionType,
		})
	}

	response := &kafka.DescribeACLsResponse{Resources: make([]kafka.ACLResource, 0, len(order))}
	for _, key := range order {
		response.Resources = append(response.Resources, *byResource[key])
	}

	return response, nil
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

			// A TIMESTAMP request, which is neither of the two sentinels. kafka-go answers it
			// in the Offsets map, and the fake must do the same or the production code would be
			// reading a field the real client never fills.
			if request.Timestamp != int64(kafka.FirstOffset) && request.Timestamp != int64(kafka.LastOffset) {
				f.timeOffsetRequests++

				if injected, ok := f.offsetErrors[topic][request.Partition]; ok {
					offsets.Error = injected
					response.Topics[topic] = append(response.Topics[topic], offsets)

					continue
				}

				if f.windowStartOmitted[topic][request.Partition] {
					// UNREADABLE: the partition is left out of the response altogether, which
					// is what the real client produces for a partition the broker did not
					// answer for. It must stay distinguishable from "nothing that recent",
					// which is an EMPTY Offsets map — reading the first as the second makes
					// the broker side short, and short is indistinguishable from loss.
					continue
				}

				offsets.Offsets = map[int64]time.Time{}
				if start, ok := f.windowStart[topic][request.Partition]; ok && start >= 0 {
					// A resolved answer arrives in the Offsets map, keyed by the offset.
					offsets.Offsets[start] = time.Unix(0, request.Timestamp*int64(time.Millisecond))
				} else {
					// NOTHING THAT RECENT. The broker answers offset -1 with timestamp -1, and
					// -1 is kafka-go's LastOffset sentinel, so the real client routes it to
					// LastOffset and leaves the Offsets map EMPTY. Modelled exactly, because
					// the production reader derives its -1 from the map being empty.
					offsets.LastOffset = -1
				}

				response.Topics[topic] = append(response.Topics[topic], offsets)

				continue
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

// Fetch serves the seeded log of one partition.
//
// It models the three properties the gateway depends on and nothing more: records arrive in
// offset order from the requested offset, the response reports the partition's watermarks,
// and a broker-side error arrives in the response rather than as a returned error — which is
// where an authorization refusal actually appears.
//
// kafka.NewRecordReader is the real client's own reader type, so the production code reads
// through the same interface it reads a live broker through; a slice would let a reader bug
// pass here and fail against Kafka.
func (f *fakeAdminClient) Fetch(
	ctx context.Context,
	req *kafka.FetchRequest,
) (*kafka.FetchResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.record("Fetch", ctx); err != nil {
		return nil, err
	}
	f.fetchRequests = append(f.fetchRequests, req)

	log := f.records[req.Topic][req.Partition]

	response := &kafka.FetchResponse{
		Topic:            req.Topic,
		Partition:        req.Partition,
		LogStartOffset:   0,
		HighWatermark:    int64(len(log)),
		LastStableOffset: int64(len(log)),
		Records:          kafka.NewRecordReader(),
	}

	if injected, ok := f.fetchErrors[req.Topic][req.Partition]; ok {
		response.Error = injected

		return response, nil
	}

	// FIRST/LAST sentinels resolved the way the broker resolves them, so a caller asking for
	// the beginning or the end is served rather than silently given nothing.
	from := req.Offset
	switch from {
	case int64(kafka.FirstOffset):
		from = 0
	case int64(kafka.LastOffset):
		from = int64(len(log))
	}

	served := make([]kafka.Record, 0, len(log))
	for _, record := range log {
		if record.Offset < from {
			continue
		}

		served = append(served, record)
	}

	response.Records = kafka.NewRecordReader(served...)

	return response, nil
}

// fetchBounds returns the requests Fetch received, so a test can assert the bounds the caller
// applied rather than only the records it kept.
func (f *fakeAdminClient) fetchBounds() []*kafka.FetchRequest {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]*kafka.FetchRequest(nil), f.fetchRequests...)
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

	const categoryCount = 4

	require.Len(t, expectedEventTopics, categoryCount*2,
		"four category topics and one dead-letter sibling each")
	assert.Equal(t, expectedEventTopics[:categoryCount], AllTopics(),
		"the first four entries are the category topics, in canonical provisioning order")
	assert.Equal(t, expectedEventTopics[categoryCount:], AllDeadLetterTopics(),
		"the last four entries are their dead-letter siblings, in the same order")

	categories := EventCategories()
	require.Len(t, categories, categoryCount,
		"four categories are what give every emitted event type a home — including the system category an unrecognised type routes to; a fifth would need a topic here and a change to the published topic contract")

	// EVERY CATEGORY TOPIC IS PROVISIONED AND GRANTABLE, and NO dead-letter sibling is.
	//
	// Provisioned, because Blnk writes to all of them. A topic nobody created is a topic the
	// relay cannot publish to, so its events would strand in the outbox.
	//
	// Grantable, because each carries event types the legacy webhook transport delivers today —
	// the system topic included, which is why ledger.created keeps an authorized route after
	// the sunset. Which of them a PARTICULAR subscriber holds is decided per subscriber by its
	// authorized_topics, not here.
	//
	// Never the dead-letter siblings: they carry failure metadata and every subscriber's failed
	// events, and are read under the master key through GET /events/dead-letter.
	for _, category := range categories {
		topic := TopicForCategory(category)
		assert.Contains(t, expectedEventTopics, topic,
			"category topic %q must be provisioned: Blnk publishes to it", topic)
		assert.True(t, IsSubscriberGrantableTopic(topic),
			"category topic %q must be grantable, or an event type the legacy transport delivers has no authorized Kafka route", topic)
		assert.False(t, IsSubscriberGrantableTopic(DLTFor(topic)),
			"dead-letter topic %q must never be grantable: it carries failure metadata and every subscriber's failed events", DLTFor(topic))
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
// PartitionKeyPrefix is deliberately ABSENT, so this is the ORDINARY subscriber: the topic
// grant is its whole boundary, the broker keeps all of it, and it holds Read and Describe on
// each authorised topic. That is the shape most tests in this file are about.
//
// It used to carry a prefix, "to prove it is carried and NOT enforced", which was accurate while
// a key scope changed nothing about the grant. It changes the grant now — a key-scoped
// subscriber is provisioned with Describe and NO Read so that Blnk's stream gateway is the only
// path its records can take — so a fixture carrying one would quietly make every binding
// assertion in this file assert the narrowed shape. testKeyScopedSubscriber is that case, named.
func testSubscriber() *model.EventSubscriber {
	principal, err := model.CanonicalKafkaPrincipal(testSubscriberID)
	if err != nil {
		panic("test fixture: the subscriber identifier must derive a principal: " + err.Error())
	}

	group, err := model.CanonicalConsumerGroupID(testSubscriberID)
	if err != nil {
		panic("test fixture: the subscriber identifier must derive a consumer group: " + err.Error())
	}

	return &model.EventSubscriber{
		SubscriberID:     testSubscriberID,
		Name:             "Acme Reconciliation",
		KafkaPrincipal:   principal,
		ConsumerGroupID:  group,
		AuthorizedTopics: []string{"blnk.transactions", "blnk.balances"},
	}
}

// testKeyScopedSubscriber returns the same row PLUS a recorded partition-key prefix.
//
// It is the fixture for the boundary Kafka cannot express: such a subscriber is granted Describe
// but not Read on its topics, so the broker refuses every record fetch and the records it is
// entitled to are delivered — key-filtered — by Blnk's subscriber stream gateway. Every test
// about that shape names this fixture, so no test asserts it by accident.
func testKeyScopedSubscriber() *model.EventSubscriber {
	prefix := "acme-"

	subscriber := testSubscriber()
	subscriber.PartitionKeyPrefix = &prefix

	return subscriber
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

// TestProvisionSubscriberPrincipal_WithholdsRecordReadFromAKeyScopedSubscriber is SEC-06, and it
// is the isolation boundary asserted binding by binding.
//
// # The exposure this closes
//
// A subscriber recording a partition-key prefix used to be provisioned with the ordinary grant:
// Read and Describe on every authorised topic. The response then echoed the prefix and declared
// that applying it was the consumer's own obligation. That was accurate prose about a boundary
// that did not exist — the principal could read every record on a shared category topic,
// including records written for other ledgers and other subscribers, and a client that ignored
// the obligation (or simply used another Kafka client) was not misbehaving in any way the
// platform could detect.
//
// # What must be true instead
//
// Describe, so the subscriber can still resolve its topics and their offsets. Read on its own
// consumer-group namespace, so the namespace stays reserved to it. And NO topic Read, so the
// broker refuses every fetch it attempts — which is what makes Blnk's stream gateway, where the
// prefix IS applied, the only path its records can take.
//
// The equality assertion is deliberate rather than a scan: this grant is smaller than the
// ordinary one, and a test that only forbade Read could be satisfied by a grant that had lost
// Describe or the group binding too, leaving a subscriber that can neither consume nor discover
// anything.
func TestProvisionSubscriberPrincipal_WithholdsRecordReadFromAKeyScopedSubscriber(t *testing.T) {
	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	subscriber := testKeyScopedSubscriber()

	result, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
	)
	require.NoError(t, err,
		"a recorded prefix must not withhold the CREDENTIAL: refusing withheld the only credential "+
			"such a row can ever have, which is the dead end that preceded the disclosure that "+
			"preceded this boundary")

	principal := testSubscriberPrincipal(t)

	expected := []kafka.ACLEntry{
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
			Operation:           kafka.ACLOperationTypeDescribe,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		},
		{
			ResourceType:        kafka.ResourceTypeGroup,
			ResourceName:        "blnk-sub-" + testSubscriberID + ".",
			ResourcePatternType: kafka.PatternTypePrefixed,
			Principal:           principal,
			Host:                ACLHostAny,
			Operation:           kafka.ACLOperationTypeRead,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		},
	}

	assert.Equal(t, expected, requestedACLs(fake),
		"a key-scoped subscriber must be granted Describe on each topic and Read on its group "+
			"namespace, and NOTHING else: a topic Read here admits it to every record on a shared "+
			"topic whatever its key")

	assert.False(t, result.TopicRecordAccessGranted,
		"and the result must SAY so, because the credential response is built from it")
	assert.True(t, result.KeyScopeBoundaryVerified,
		"the boundary is verified before the password becomes returnable, not assumed from the "+
			"code that built the bindings")
	assert.True(t, result.AuthorizerActive,
		"withholding Read withholds nothing on a broker that enforces no ACLs, so enforcement is "+
			"part of the same verification")

	// The ordinary subscriber is unchanged, which is the other half of the property: this is a
	// narrowing of one case, not of the access model.
	ordinaryFake := newFakeAdminClient()
	ordinaryAdmin := newTestKafkaAdmin(ordinaryFake, MinTopicPartitions, 1)

	ordinary, err := ordinaryAdmin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword),
	)
	require.NoError(t, err)
	assert.True(t, ordinary.TopicRecordAccessGranted,
		"a subscriber with no prefix keeps direct broker consumption: its topic grant IS its "+
			"boundary and the broker keeps all of it")
	assert.False(t, ordinary.KeyScopeBoundaryVerified,
		"and it has no key boundary to verify, which is a different fact from an unverified one")
}

// TestProvisionSubscriberPrincipal_NarrowsAPreviouslyWideKeyScopedGrant proves the transition an
// operator actually performs: a subscriber is provisioned, a prefix is recorded, and the grant
// that already exists at the broker has to LOSE its record access.
//
// Creating the narrower binding set without removing the wider one would leave the subscriber
// exactly as exposed as before while every response reported the boundary as enforced — the
// worst of the three states, because it is the one that looks fixed.
func TestProvisionSubscriberPrincipal_NarrowsAPreviouslyWideKeyScopedGrant(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	// First issuance: no prefix recorded, so the ordinary grant is written.
	_, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword),
	)
	require.NoError(t, err)
	require.Len(t, fake.heldBindings(), 5, "two topics Read+Describe plus the group binding")

	// A prefix is then recorded, and the credential is reissued.
	result, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(testKeyScopedSubscriber(), sentinelPassword),
	)
	require.NoError(t, err)

	assert.Equal(t, 2, result.ACLBindingsRemoved,
		"the two topic Read bindings must be DELETED, not merely left unrequested")

	for _, held := range fake.heldBindings() {
		if held.ResourceType != kafka.ResourceTypeTopic {
			continue
		}
		assert.NotEqual(t, kafka.ACLOperationTypeRead, held.Operation,
			"no topic Read may survive on a key-scoped principal; a surviving one is the "+
				"whole-topic access the prefix is supposed to deny")
	}

	assert.Len(t, fake.heldBindings(), 3,
		"the broker must hold exactly Describe on each topic plus the group binding")
}

// TestVerifyKeyScopeBoundary_RefusesEveryWayTheBoundaryCanBeAbsent pins the verification itself,
// independently of the provisioning call that runs it.
//
// Each case is a distinct way a key-scoped subscriber could end up able to read records it is not
// entitled to, and all three must refuse — the credential is revoked and no password is returned.
// The last case is the one that could not be reached through aclEntries: a hand-made ALLOW
// binding restoring the access Blnk withheld.
func TestVerifyKeyScopeBoundary_RefusesEveryWayTheBoundaryCanBeAbsent(t *testing.T) {
	scoped := NewSubscriberProvisioningRequest(testKeyScopedSubscriber(), sentinelPassword)
	narrow := scoped.aclEntries()
	reconciliation := SubscriberACLReconciliation{Principal: scoped.Principal}

	require.NoError(t, verifyKeyScopeBoundary(scoped, narrow, reconciliation, true),
		"the narrowed grant on an enforcing broker with no foreign binding is the boundary")

	t.Run("a topic Read among the bindings", func(t *testing.T) {
		wide := append([]kafka.ACLEntry(nil), narrow...)
		wide = append(wide, kafka.ACLEntry{
			ResourceType:        kafka.ResourceTypeTopic,
			ResourceName:        "blnk.transactions",
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           kafkaPrincipalPrefix + scoped.Principal,
			Host:                ACLHostAny,
			Operation:           kafka.ACLOperationTypeRead,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		})

		err := verifyKeyScopeBoundary(scoped, wide, reconciliation, true)
		require.ErrorIs(t, err, ErrSubscriberKeyScopeUnenforced,
			"a grant carrying Read admits the principal to every record on the topic, so the "+
				"prefix bounds nothing")
	})

	t.Run("a broker whose enforcement is unconfirmed", func(t *testing.T) {
		err := verifyKeyScopeBoundary(scoped, narrow, reconciliation, false)
		require.ErrorIs(t, err, ErrSubscriberKeyScopeUnenforced,
			"withholding Read withholds nothing where no authorizer evaluates the bindings")
	})

	t.Run("a foreign ALLOW binding on the principal", func(t *testing.T) {
		widened := SubscriberACLReconciliation{
			Principal:    scoped.Principal,
			ForeignAllow: []string{"Allow Read on Topic \"blnk.transactions\" (Literal)"},
		}

		err := verifyKeyScopeBoundary(scoped, narrow, widened, true)
		require.ErrorIs(t, err, ErrSubscriberKeyScopeUnenforced,
			"a binding Blnk did not create may restore exactly the access this boundary withholds")
	})

	t.Run("a subscriber with no key scope is verified vacuously", func(t *testing.T) {
		ordinary := NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword)

		require.NoError(t, verifyKeyScopeBoundary(ordinary, ordinary.aclEntries(), reconciliation, true),
			"there is no key boundary to keep, and the topic grant the broker enforces is the "+
				"whole of what such a subscriber asked for")
	})
}

// TestBindingsGrantTopicRead_ReadsTheBindingsRatherThanTheRequest pins the leaf the verification
// and the result field both rest on.
//
// It answers over the bindings on purpose: derived from the request instead, it would be a
// restatement of KeyScoped and could not catch the case it exists for — a binding set that does
// not match the decision the request asked for.
func TestBindingsGrantTopicRead_ReadsTheBindingsRatherThanTheRequest(t *testing.T) {
	assert.False(t, bindingsGrantTopicRead(nil),
		"a principal granted nothing can read nothing")

	assert.True(t, bindingsGrantTopicRead(
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword).aclEntries()))
	assert.False(t, bindingsGrantTopicRead(
		NewSubscriberProvisioningRequest(testKeyScopedSubscriber(), sentinelPassword).aclEntries()))

	// A GROUP Read is not a record grant, and reading it as one would refuse every key-scoped
	// subscriber — the group binding is retained for them precisely so the namespace stays
	// reserved.
	assert.False(t, bindingsGrantTopicRead([]kafka.ACLEntry{{
		ResourceType:   kafka.ResourceTypeGroup,
		ResourceName:   "blnk-sub-x.",
		Operation:      kafka.ACLOperationTypeRead,
		PermissionType: kafka.ACLPermissionTypeAllow,
	}}))

	// A DENY takes access away. Counting it as a grant would refuse issuance over a binding that
	// makes the subscriber narrower, not wider.
	assert.False(t, bindingsGrantTopicRead([]kafka.ACLEntry{{
		ResourceType:   kafka.ResourceTypeTopic,
		ResourceName:   "blnk.transactions",
		Operation:      kafka.ACLOperationTypeRead,
		PermissionType: kafka.ACLPermissionTypeDeny,
	}}))
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
	// inventory: all four category topics are grantable and no dead-letter topic is, so asking
	// for a DLT is refused before any binding is built (see
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
// The access model has NO per-tenant topics: every subscriber reads from the same shared
// inventory — the three subscriber-facing categories out of the five Blnk owns — so the only
// thing keeping one subscriber out of another's data is this ACL grant. That means the boundary
// has to be asserted from the
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
			rendered := strings.TrimSpace(
				strings.Join([]string{key, renderAdminLogField(value)}, "="),
			)
			assert.NotContains(t, rendered, sentinelPassword,
				"log field %q must never contain the SCRAM password", key)
		}
	}
}

// renderAdminLogField renders a structured log field value for substring inspection.
//
// # Why the name is qualified
//
// It was called `format`, which is one of the most ordinary identifiers a Go file can declare
// and was declared at package scope in a test binary that compiles EVERY root-package test file
// together. A second `format` helper written in any other root test file — for a load-test
// summary, a topic name, a duration — would not merely shadow this one, it would fail to
// compile the whole binary with a redeclaration error, and the reader of that error would be
// looking at their own new file rather than at a password-inspection helper eight thousand
// lines away in another. The name now says which subject it belongs to, which is the convention
// every other shared helper in this file already follows (`fakeACLKey`, `newTestKafkaAdmin`,
// `parseEventAdminSource`).
func renderAdminLogField(value interface{}) string {
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
// and every dead-letter topic, which carries Blnk's failure metadata and every other
// subscriber's failed events and has no subscriber audience.
func TestProvisionSubscriberPrincipal_RefusesATopicOutsideTheGrantableAllowlist(t *testing.T) {
	cases := map[string]string{
		"the wildcard":            "*",
		"a foreign topic":         "attacker.transactions",
		"a dead-letter topic":     "blnk.transactions.dlt",
		"the system dead-letter":  "blnk.system.dlt",
		"a retired category name": "blnk.ledgers",
		"an internal Kafka topic": "__consumer_offsets",
		"a prefix fragment":       "blnk.",
		"the prefix alone":        "blnk",
		// Ledger events are published, but to blnk.system, so a ledgers topic is a name
		// nothing creates and nobody may be granted.
		"a category this contract does not have": "blnk.ledgers",
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

// THE AUTH-01 COMPENSATION CONTRACT IS ASSERTED IN ONE PLACE:
// TestProvisionSubscriberPrincipal_ReportsWhatCompensationActuallyAchieved, further down this
// file.
//
// Two tests used to sit here — RevokesTheCredentialWhenBindingFails and
// ReportsWhenCompensationItselfFails — asserting the same two outcomes that suite's first two
// cases assert, on the same fixture, with the same injected failures. Three statements of one
// contract is how the contract comes to be stated three DIFFERENT ways: a case added to one and
// not the others reads as a deliberate distinction rather than the omission it is. The bound
// suite carries the caller-cancellation cases too, which is the axis none of the three covered.

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
// the mapping, including the one field whose VALUE is deliberately not mapped.
//
// Kafka's authorizer has no message-key dimension, so the prefix's value cannot appear in any
// ACL. Expressing it as a PREFIXED topic pattern — the only binding that looks like it might fit
// — would WIDEN the topic grant to every topic sharing the prefix while appearing to narrow it,
// which is strictly worse than not enforcing it at all.
//
// Its PRESENCE is mapped, onto KeyScoped, and that is what makes the boundary real: a key-scoped
// request withholds record-level Read so that Blnk's stream gateway is the only path records can
// take. Both halves are asserted here, because mapping neither leaves the prefix unenforced and
// mapping the value would widen the grant.
func TestNewSubscriberProvisioningRequest_MapsTheRegistryRowAndNotThePartitionKeyPrefix(t *testing.T) {
	subscriber := testKeyScopedSubscriber()

	request := NewSubscriberProvisioningRequest(subscriber, sentinelPassword)

	assert.Equal(t, subscriber.SubscriberID, request.SubscriberID)
	assert.Equal(t, subscriber.KafkaPrincipal, request.Principal)
	assert.Equal(t, subscriber.ConsumerGroupID, request.ConsumerGroupPrefix)
	assert.Equal(t, subscriber.AuthorizedTopics, request.Topics)
	assert.True(t, request.KeyScoped,
		"the PRESENCE of a prefix must reach the request, because it decides whether record-level "+
			"Read is granted at all")
	assert.False(t, NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword).KeyScoped,
		"and a row with no prefix must not be narrowed: its topic grant is its whole boundary")
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
		assert.NotEqual(t, kafka.ACLOperationTypeRead, entry.Operation,
			"and a key-scoped subscriber must hold no topic Read at all: granting it would admit "+
				"the principal to every record on the shared topic, which is the exposure the "+
				"gateway exists to close")
	}

	// AND THE REQUEST IS VALID, precisely BECAUSE of the narrowing above. A declared key scope has
	// no broker representation, so what makes such a request safe is that it asks for none: no
	// topic Read is requested, every direct fetch is refused by the broker, and the records are
	// delivered key-filtered by the stream gateway.
	require.NoError(t, request.validate(),
		"a key-scoped request that withholds record-level Read must be provisionable: refusing it "+
			"withholds the credential instead of narrowing it, and leaves the third dimension of "+
			"requirement R-7's access model with no working path at all")

	// WHAT IS REFUSED IS THE PAIR THAT CANNOT BE TRUE AT ONCE: a request naming a key scope while
	// asking for the ordinary, unnarrowed grant. NewSubscriberProvisioningRequest cannot produce
	// it — it sets both fields from one row — but this type is exported, so a request assembled by
	// hand is exactly how the enforcement point would be defeated through an ordinary authorized
	// call. The credential it would mint reads every record on both granted topics beneath a row
	// saying it may see one ledger's.
	forged := request
	forged.KeyScoped = false
	require.Error(t, forged.validate(),
		"a request that declares a key scope it does not narrow the grant for must fail "+
			"validation, or the admin layer mints access wider than the registry row describes")
	assert.Contains(t, forged.validate().Error(), "no message-key dimension",
		"the refusal must say why no ACL can express the prefix")
	assert.Contains(t, forged.validate().Error(), *subscriber.PartitionKeyPrefix,
		"and must name the prefix, so an operator can see which value it was asked to enforce")

	assert.Equal(t, SubscriberProvisioningRequest{Password: NewSubscriberSecret(sentinelPassword)},
		NewSubscriberProvisioningRequest(nil, sentinelPassword),
		"a nil registry row must produce a request that fails validation rather than a panic")
}

// TestProvisionSubscriberPrincipal_RefusesAKeyScopeItWouldNotNarrow is the admin layer's half of
// the fail-closed rule, and it asserts the part that matters: NOTHING reaches the broker.
//
// A key-scoped row IS provisionable — its bindings are Describe on the topics and Read on the
// consumer group, with no record-level Read anywhere, and the stream gateway delivers its records
// key-filtered. What must never be provisioned is a request that names the prefix while asking for
// the ordinary grant, because the credential it mints reads every record on every granted topic,
// including other ledgers' and other subscribers', beneath a registry row saying it may see one
// ledger's.
//
// NewSubscriberProvisioningRequest cannot build that pair. This is the barrier on the exported
// function that any other caller — a CLI, a repair script, a future endpoint — would reach the
// broker through, and it is where a request assembled by hand arrives.
func TestProvisionSubscriberPrincipal_RefusesAKeyScopeItWouldNotNarrow(t *testing.T) {
	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	subscriber := testSubscriber()
	prefix := "ldg_9f2c"
	subscriber.PartitionKeyPrefix = &prefix

	forged := NewSubscriberProvisioningRequest(subscriber, sentinelPassword)
	require.True(t, forged.KeyScoped, "the premise: the constructor narrows a key-scoped row")
	forged.KeyScoped = false

	result, err := admin.ProvisionSubscriberPrincipal(context.Background(), forged)

	require.Error(t, err, "a key scope the request does not narrow the grant for must be refused")
	assert.Contains(t, err.Error(), "records the partition key prefix")
	assert.Contains(t, err.Error(), prefix,
		"the refusal must name the prefix, so an operator can see which value blocked it")

	assert.False(t, result.CredentialWritten,
		"no credential may be reported written by a refusal that never reached the broker")
	assert.False(t, result.CompensationOwed,
		"and nothing can be owed, because nothing was done")
	assert.Zero(t, fake.callCount("AlterUserScramCredentials"),
		"the refusal must happen BEFORE the credential round trip")
	assert.Zero(t, fake.callCount("CreateACLs"),
		"and before any binding is created")

	// BOTH REMEDIES THE REFUSAL NAMES MUST WORK, or it is a dead end.
	//
	// Building the request through the constructor, which narrows the grant:
	_, err = admin.ProvisionSubscriberPrincipal(
		context.Background(), NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
	)
	require.NoError(t, err,
		"the same row is provisionable through the constructor, which withholds record-level Read")

	// Or clearing the prefix, which accepts whole-topic access:
	subscriber.PartitionKeyPrefix = nil
	_, err = admin.ProvisionSubscriberPrincipal(
		context.Background(), NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
	)
	require.NoError(t, err,
		"and clearing the prefix makes the identical row provisionable on the ordinary terms")
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

	records := report.LagSamples()
	require.Len(t, records, 2, "the inventory must carry one sample per topic")
	// The two identity labels are PSEUDONYMS, not identifiers. A metric label is the most
	// widely readable thing this process emits — scraped into a time-series database,
	// rendered on dashboards, quoted into alert notifications and forwarded to whatever
	// receives them — and a subscriber id is a tenant name. So each is a stable truncated
	// hash: one subscriber is still exactly one series, which is all monitoring needs, and
	// correlating a series back to a tenant stays possible for whoever holds the registry.
	//
	// Asserted against the resolvers rather than against a literal digest, deliberately.
	// A literal here would pin the hash construction as well as the pseudonymity, so
	// changing the digest would fail this test for a reason it is not about; and the
	// resolvers are the SINGLE source both the publish path and the clear path use, which is
	// the property that actually matters — a clear that resolved a label differently would
	// zero a tuple nobody published and leave the real series standing for ever.
	wantSubscriber := subscriberLagLabel("sub_0f6e2c8a")
	wantGroup := consumerGroupLagLabel("blnk-sub-sub_0f6e2c8a.recon")
	require.NotEqual(t, "sub_0f6e2c8a", wantSubscriber,
		"the subscriber label must not be the raw identifier: that is the disclosure the "+
			"pseudonym exists to prevent")
	require.NotContains(t, wantGroup, "sub_0f6e2c8a",
		"and the group label must not carry it either, or hashing the subscriber label beside "+
			"it is defeated by the other route")

	for _, record := range records {
		assert.Equal(t, wantSubscriber, record.Subscriber,
			"the alert rule interpolates $labels.subscriber, so it must be the resolver's value")
		// The SUBSCRIBER-SCOPED ROOT, not the full group id, then hashed. A subscriber that
		// runs several consumer groups is one subscriber to an operator triaging the alert,
		// and collapsing the leaf before hashing is what keeps its lag on one series instead
		// of one series per group it happens to be running.
		assert.Equal(t, wantGroup, record.Group,
			"the alert rule interpolates $labels.group")
		assert.NotEmpty(t, record.Topic, "the alert rule interpolates $labels.topic")
		assert.GreaterOrEqual(t, record.Lag, int64(0), "no negative value may ever reach the gauge")
	}
	assert.Equal(t, int64(40), records[0].Lag)
	assert.Equal(t, int64(6), records[1].Lag)
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

// TestConsumerLag_WithholdsLagForAPartitionWhoseCommittedOffsetTheBrokerRefused is the
// END-TO-END guard on OBS-21, driven through the real ConsumerLag path with a broker that
// answers ListOffsets for every partition and refuses OffsetFetch for one of them.
//
// # The failure this rules out
//
// A per-partition error in the OffsetFetch response used to be skipped, which left the entry
// absent, which the lookup reported as -1, which the arithmetic treats as "the group has never
// committed" — a deliberate FULL-LAG policy. So refusing to report one partition of a
// caught-up consumer produced the entire retained log as that partition's lag, on a topic that
// stayed marked COMPLETE, and that number went to the gauge SubscriberConsumerLagHigh fires on.
// The alert an operator received would name a six-figure backlog that did not exist, while
// ConsumerLagMeasurementDegraded — the rule that exists to cover an unmeasurable subject — said
// nothing, because nothing had reported a degradation.
//
// The fake has been able to inject this since it was written; no test had ever asked it to,
// which is precisely how the defect survived every other lag assertion in this file.
func TestConsumerLag_WithholdsLagForAPartitionWhoseCommittedOffsetTheBrokerRefused(t *testing.T) {
	fake := newFakeAdminClient()
	// Both partitions are readable on the END-OFFSET side, with a large retained log so a
	// full-lag misscoring would be unmistakable.
	fake.withOffsets("blnk.transactions", 0, 1000, 900000).withCommitted("blnk.transactions", 0, 899990)
	fake.withOffsets("blnk.transactions", 1, 1000, 900000).withCommitted("blnk.transactions", 1, 899995)
	// ...and the broker refuses the committed offset for exactly one of them.
	fake.committedErrors["blnk.transactions"] = map[int]error{1: kafka.NotCoordinatorForGroup}

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{
		SubscriberID: "sub_11111111-1111-1111-1111-111111111111",
		GroupID:      "sub_11111111-1111-1111-1111-111111111111-group",
		Topics:       []string{"blnk.transactions"},
	})
	require.NoError(t, err, "one refused partition must not void the whole measurement")

	require.Len(t, report.Topics, 1)
	topicLag := report.Topics[0]

	assert.Equal(t, int64(10), report.TotalLag,
		"only the partition whose committed offset was actually read may contribute: the total is a "+
			"LOWER BOUND, not the 899,000 a full-lag misscoring would have invented")
	assert.Equal(t, 1, topicLag.PartitionsUnavailable,
		"the refused partition must raise the degraded-measurement count, which is the signal "+
			"ConsumerLagMeasurementDegraded fires on")
	assert.Zero(t, topicLag.PartitionsWithoutCommit,
		"and it must not ALSO be diagnosed as a partition the group never committed on — that reading "+
			"sends an operator to the consumer instead of to the broker")

	require.Len(t, topicLag.Partitions, 2)
	assert.False(t, topicLag.Partitions[0].Unavailable)
	assert.Equal(t, int64(10), topicLag.Partitions[0].Lag)
	assert.True(t, topicLag.Partitions[1].Unavailable,
		"an unreadable committed offset makes the partition unmeasurable, exactly as an unreadable "+
			"end offset does")
	assert.Zero(t, topicLag.Partitions[1].Lag)
	assert.False(t, topicLag.Partitions[1].Committed,
		"nothing was read, so no commit may be claimed either way")

	// The consequence that matters operationally: the partial total is WITHHELD from the gauge
	// the threshold rule reads, and the unmeasured-partition count is published in its place.
	samples := report.LagSamples()
	require.Len(t, samples, 1)
	assert.False(t, samples[0].LagComplete,
		"a topic measured from a partial set of committed offsets must be marked incomplete")
	assert.Equal(t, 1, samples[0].UnmeasuredPartitions)
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
	assert.Empty(t, report.LagSamples(), "no sample may be exported for a measurement that did not happen")
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

		records := report.LagSamples()
		require.Len(t, records, 1, "one topic measured, one gauge value published")
		assert.Equal(t, int64(24_000), records[0].Lag,
			"the gauge must carry the aggregate undiminished; a truncated value would silence the alert")
		assert.Greater(t, records[0].Lag, alertThreshold,
			"the published value must cross the threshold the rule fires on")
	})

	t.Run("offsets near the int64 ceiling neither wrap nor saturate", func(t *testing.T) {
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

		records := report.LagSamples()
		require.Len(t, records, 2)
		for _, record := range records {
			assert.Equal(t, behind, record.Lag,
				"the gauge must receive the exact figure at this scale")
			assert.GreaterOrEqual(t, record.Lag, int64(0),
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

	report, err := admin.TopicEndOffsets(context.Background(), time.Time{})
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

	report, err := admin.TopicEndOffsets(context.Background(), time.Time{}, "blnk.transactions", "blnk.transactions.dlt")
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

	report, err := admin.TopicEndOffsets(context.Background(), time.Time{}, "blnk.transactions", "blnk.absent")
	require.NoError(t, err)

	assert.Equal(t, []string{"blnk.absent"}, report.MissingTopics)
	assert.Equal(t, 1, report.PartitionsUnavailable)
	assert.Equal(t, int64(10), report.EndOffsetSum, "an unreadable partition contributes nothing to the sum")

	snapshot, found := report.Lookup("blnk.transactions")
	require.True(t, found)
	require.Len(t, snapshot.Partitions, 2)
	assert.True(t, snapshot.Partitions[1].Unavailable)
}

// TestTopicEndOffsets_ReadsAWindowOnlyWhenOneIsAskedFor is the PERF-P05 guard on the broker
// side of the reconciliation.
//
// The comparison needs both sides counted over the SAME population. Cumulative end offsets are
// not one: they count records retention has already deleted, while the outbox forgets, so the
// tolerated surplus grows by however much the outbox has forgotten until it can conceal any
// amount of loss. A window-start offset per partition is what bounds the broker side, and it is
// a timestamp lookup — a different question from the two sentinel offsets, answered in a
// different field.
//
// It is read in its OWN round trip and only when a window was asked for, because the cached
// bounds the lag sweep shares must not be invalidated or enlarged by a reconciliation that runs
// once a day.
func TestTopicEndOffsets_ReadsAWindowOnlyWhenOneIsAskedFor(t *testing.T) {
	since := time.Now().UTC().Add(-2 * time.Hour)

	t.Run("no window asked for reads no window", func(t *testing.T) {
		fake := newFakeAdminClient()
		fake.withOffsets("blnk.transactions", 0, 0, 100)
		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		report, err := admin.TopicEndOffsets(context.Background(), time.Time{}, "blnk.transactions")
		require.NoError(t, err)

		assert.True(t, report.WindowStart.IsZero(),
			"a caller that named no window must get none, so the verdict is reported diagnostic-only")
		assert.Zero(t, report.WindowRecordCount)
		assert.Zero(t, fake.snapshotTimeOffsetRequests(),
			"and no timestamp lookup may be issued: it would cost a round trip whose answer nobody reads")
	})

	t.Run("a window counts the records written inside it", func(t *testing.T) {
		fake := newFakeAdminClient()
		// Partition 0: 100 records, the window starting at offset 90 — ten inside.
		fake.withOffsets("blnk.transactions", 0, 0, 100).withWindowStart("blnk.transactions", 0, 90)
		// Partition 1: 50 records, the window starting at offset 45 — five inside.
		fake.withOffsets("blnk.transactions", 1, 0, 50).withWindowStart("blnk.transactions", 1, 45)

		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		report, err := admin.TopicEndOffsets(context.Background(), since, "blnk.transactions")
		require.NoError(t, err)

		assert.Equal(t, since, report.WindowStart)
		assert.Equal(t, int64(15), report.WindowRecordCount,
			"the windowed figure is what the reconciliation compares against, and it is a fraction "+
				"of the 150 cumulative records")
		assert.Equal(t, int64(150), report.EndOffsetSum,
			"the cumulative sum is still reported, because the diagnostic reading and the per-topic "+
				"projection both use it")
		assert.False(t, report.WindowTruncated)
		assert.Zero(t, report.WindowPartitionsUnreadable)

		snapshot, found := report.Lookup("blnk.transactions")
		require.True(t, found)
		assert.Equal(t, int64(90), snapshot.Partitions[0].WindowStartOffset)
		assert.Equal(t, int64(45), snapshot.Partitions[1].WindowStartOffset)

		// TWO round trips, not one enlarged one: the window is asked about separately so the
		// bounds request the lag sweep caches stays exactly what it was.
		assert.Equal(t, 2, fake.callCount("ListOffsets"),
			"the window is its own request, so the cached bounds the lag sweep shares are untouched")
		assert.Equal(t, 2, fake.snapshotTimeOffsetRequests(), "one timestamp lookup per partition")
	})

	t.Run("a partition holding nothing that recent contributes a real zero", func(t *testing.T) {
		fake := newFakeAdminClient()
		fake.withOffsets("blnk.transactions", 0, 0, 100).withWindowStart("blnk.transactions", 0, -1)

		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		report, err := admin.TopicEndOffsets(context.Background(), since, "blnk.transactions")
		require.NoError(t, err)

		assert.Zero(t, report.WindowRecordCount,
			"every record predates the window, which is a genuine zero and not a failure to read")
		assert.Zero(t, report.WindowPartitionsUnreadable)
		assert.False(t, report.WindowTruncated)
		assert.False(t, report.WindowStart.IsZero(), "the window itself was still measured")
	})

	t.Run("an unreadable window start is reported, never counted as zero", func(t *testing.T) {
		// THE DISTINCTION THIS EXISTS FOR. "Nothing that recent" and "could not be read" both
		// contribute no records, and treating the second as the first makes the broker side
		// short — which is indistinguishable from message loss, and is reported as loss.
		fake := newFakeAdminClient()
		fake.withOffsets("blnk.transactions", 0, 0, 100).withWindowStart("blnk.transactions", 0, 90)
		fake.withOffsets("blnk.transactions", 1, 0, 100).withUnreadableWindow("blnk.transactions", 1)

		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		report, err := admin.TopicEndOffsets(context.Background(), since, "blnk.transactions")
		require.NoError(t, err)

		assert.Equal(t, 1, report.WindowPartitionsUnreadable,
			"the partition that could not be read must be COUNTED as unreadable")
		assert.Equal(t, int64(10), report.WindowRecordCount,
			"and must contribute nothing, so the shortfall it causes is explained rather than silent")

		snapshot, found := report.Lookup("blnk.transactions")
		require.True(t, found)
		assert.True(t, snapshot.Partitions[1].WindowUnreadable)
		assert.Equal(t, int64(-1), snapshot.Partitions[1].WindowStartOffset)

		// The verdict must decline to conclude, which is the whole purpose of counting it.
		verdict := ReconcileAgainstOutbox(report, model.EventRecordIntervalAudit{
			PublishedRows: 10, CorroboratedRows: 10, DistinctCorroboratedRecords: 10, WindowStart: since,
		})
		assert.False(t, verdict.Conclusive)
		assert.Contains(t, strings.Join(verdict.Caveats, " "), "window-start offset")
	})

	t.Run("retention inside the window is detected and reported", func(t *testing.T) {
		// The window resolves to an offset at or below the oldest record the partition still
		// holds, which means records written INSIDE the window have been deleted. That is the
		// only case in which retention invalidates a windowed comparison, and it is the case
		// the old unconditional retention caveat could not distinguish from healthy operation.
		fake := newFakeAdminClient()
		fake.withOffsets("blnk.transactions", 0, 60, 100).withWindowStart("blnk.transactions", 0, 60)

		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		report, err := admin.TopicEndOffsets(context.Background(), since, "blnk.transactions")
		require.NoError(t, err)

		assert.True(t, report.WindowTruncated,
			"the oldest retained record is already inside the window, so earlier ones inside it are gone")
		assert.Equal(t, int64(40), report.WindowRecordCount,
			"what remains is still counted, so the figure is a lower bound rather than nothing")

		verdict := ReconcileAgainstOutbox(report, model.EventRecordIntervalAudit{
			PublishedRows: 40, CorroboratedRows: 40, DistinctCorroboratedRecords: 40, WindowStart: since,
		})
		assert.False(t, verdict.Conclusive)
		assert.Contains(t, strings.Join(verdict.Caveats, " "), "inside the measured window")
	})

	t.Run("no partition answering leaves the cumulative report usable", func(t *testing.T) {
		// A window that could not be read anywhere must not void the whole measurement: the
		// cumulative figures are still true and are what the diagnostic reading wants. The
		// verdict then declines to be conclusive, which is the honest outcome of a window
		// nobody could measure.
		fake := newFakeAdminClient()
		fake.withOffsets("blnk.transactions", 0, 0, 100).withUnreadableWindow("blnk.transactions", 0)
		fake.withOffsets("blnk.transactions", 1, 0, 50).withUnreadableWindow("blnk.transactions", 1)

		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		report, err := admin.TopicEndOffsets(context.Background(), since, "blnk.transactions")
		require.NoError(t, err, "a window that could not be read is not a failure of the measurement")

		assert.Equal(t, int64(150), report.EndOffsetSum, "the cumulative figures survive")
		assert.Equal(t, 2, report.WindowPartitionsUnreadable)
		assert.Zero(t, report.WindowRecordCount)

		verdict := ReconcileAgainstOutbox(report, model.EventRecordIntervalAudit{
			PublishedRows: 150, CorroboratedRows: 150, DistinctCorroboratedRecords: 150, WindowStart: since,
		})
		assert.False(t, verdict.Conclusive,
			"a windowed comparison whose window nothing answered for cannot be concluded from")
	})
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
// commit" in play, and keeps it DISTINCT from "the broker would not say".
//
// The two used to share the -1 sentinel, which is how an unreadable partition inherited the
// full-lag treatment that belongs only to a group that has genuinely never committed. Both are
// asserted here so the distinction cannot be collapsed again by a lookup that returns a bare
// integer.
func TestCommittedOffsetFor_ReportsTheBrokersOwnSentinel(t *testing.T) {
	committed := map[string]map[int]committedOffset{
		"blnk.transactions": {
			0: {offset: 42},
			2: {offset: -1, unavailable: true},
		},
	}

	present := committedOffsetFor(committed, "blnk.transactions", 0)
	assert.Equal(t, int64(42), present.offset)
	assert.False(t, present.unavailable, "a reported commit is available")

	uncommitted := committedOffsetFor(committed, "blnk.transactions", 1)
	assert.Equal(t, int64(-1), uncommitted.offset,
		"an uncommitted partition must read as the broker's own -1")
	assert.False(t, uncommitted.unavailable,
		"an absent entry is KNOWLEDGE — the group has not committed — and must not be reported as "+
			"unmeasurable, or an unstarted consumer would stop scoring as full lag")

	unreadable := committedOffsetFor(committed, "blnk.transactions", 2)
	assert.True(t, unreadable.unavailable,
		"a partition the broker refused to report is the ABSENCE of knowledge and must be marked as such")

	assert.Equal(t, int64(-1), committedOffsetFor(committed, "blnk.balances", 0).offset)
	assert.False(t, committedOffsetFor(committed, "blnk.balances", 0).unavailable)
	assert.Equal(t, int64(-1), committedOffsetFor(nil, "blnk.transactions", 0).offset)
	assert.False(t, committedOffsetFor(nil, "blnk.transactions", 0).unavailable)
}

// TestBuildPartitionLag_WithholdsLagWhenTheCommittedOffsetIsUnreadable is the direct guard on
// OBS-21, and it is the assertion the previous shape could not have satisfied.
//
// # What went wrong
//
// A per-partition error in the OffsetFetch response was skipped, so the partition's entry was
// absent, so committedOffsetFor returned -1, so the lag arithmetic applied its FULL-LAG policy:
// baseline = earliest retained offset, lag = every record still on the log. The partition stayed
// marked available, the topic stayed marked complete, and that fabricated number went to
// blnk_kafka_consumer_lag — the series SubscriberConsumerLagHigh fires on. A leader election on
// one partition of a healthy, caught-up consumer could page an operator with a six-figure lag
// that never existed, and ConsumerLagMeasurementDegraded — the rule whose whole purpose is to
// cover an unmeasurable subject — stayed silent because nothing had reported a degradation.
//
// # What must be true instead
//
// No reading, no lag. The partition is unavailable, it contributes zero to the total, and it is
// NOT counted as a partition without a commit, because "the group has not committed" is a claim
// this measurement is in no position to make.
func TestBuildPartitionLag_WithholdsLagWhenTheCommittedOffsetIsUnreadable(t *testing.T) {
	bounds := partitionOffsetBounds{first: 1000, end: 900000}

	t.Run("an unreadable committed offset withholds the lag", func(t *testing.T) {
		lag := buildPartitionLag("blnk.transactions", 3, bounds,
			committedOffset{offset: -1, unavailable: true})

		assert.True(t, lag.Unavailable,
			"an unreadable committed offset makes the partition unmeasurable, exactly as an "+
				"unreadable end offset does")
		assert.Zero(t, lag.Lag,
			"the full-lag policy belongs to a group that has genuinely never committed; applying it "+
				"here invents the largest number the partition could possibly carry")
		assert.False(t, lag.Committed,
			"nothing was read, so no commit may be claimed either way")
	})

	t.Run("a genuinely uncommitted partition still scores full lag", func(t *testing.T) {
		lag := buildPartitionLag("blnk.transactions", 3, bounds, committedOffset{offset: -1})

		assert.False(t, lag.Unavailable, "this partition WAS measured; the group simply has no commit")
		assert.Equal(t, int64(899000), lag.Lag,
			"full lag from the earliest RETAINED offset, which is what stops an unstarted consumer "+
				"from reading as perfectly healthy")
		assert.False(t, lag.Committed)
	})

	t.Run("a committed partition is unaffected", func(t *testing.T) {
		lag := buildPartitionLag("blnk.transactions", 3, bounds, committedOffset{offset: 899500})

		assert.False(t, lag.Unavailable)
		assert.True(t, lag.Committed)
		assert.Equal(t, int64(500), lag.Lag)
	})

	t.Run("an unreadable end offset still withholds the lag", func(t *testing.T) {
		lag := buildPartitionLag("blnk.transactions", 3,
			partitionOffsetBounds{first: -1, end: -1, unavailable: true},
			committedOffset{offset: 42})

		assert.True(t, lag.Unavailable, "the pre-existing half of the rule must not regress")
		assert.Zero(t, lag.Lag)
	})
}

// TestTopicLag_CountsAnUnreadableCommittedOffsetAsUnavailableRatherThanUncommitted asserts the
// aggregate consequences of the fix: the topic's measurement is INCOMPLETE, so LagSamples
// withholds the lag from the alerting gauge and publishes the unmeasured-partition count
// instead, and the partition is not double-diagnosed as a missing commit.
func TestTopicLag_CountsAnUnreadableCommittedOffsetAsUnavailableRatherThanUncommitted(t *testing.T) {
	bounds := partitionOffsetBounds{first: 0, end: 10}

	topicLag := TopicLag{Topic: "blnk.transactions"}
	for _, reading := range []committedOffset{
		{offset: 8},                     // measured, 2 behind
		{offset: -1, unavailable: true}, // the broker refused
	} {
		partitionLag := buildPartitionLag(topicLag.Topic, len(topicLag.Partitions), bounds, reading)
		topicLag.TotalLag += partitionLag.Lag
		if partitionLag.Unavailable {
			topicLag.PartitionsUnavailable++
		} else if !partitionLag.Committed {
			topicLag.PartitionsWithoutCommit++
		}
		topicLag.Partitions = append(topicLag.Partitions, partitionLag)
	}

	assert.Equal(t, int64(2), topicLag.TotalLag,
		"only the partition that was actually measured contributes, so the total is a LOWER BOUND "+
			"rather than a fabricated maximum")
	assert.Equal(t, 1, topicLag.PartitionsUnavailable,
		"the unreadable partition must raise the degraded-measurement count, which is what makes "+
			"ConsumerLagMeasurementDegraded fire")
	assert.Zero(t, topicLag.PartitionsWithoutCommit,
		"and it must NOT also be diagnosed as a partition the group never committed on, which "+
			"would send an operator to the consumer instead of to the broker")

	sample := consumerLagSample("sub_1", "sub_1-group", topicLag)
	assert.False(t, sample.LagComplete,
		"an incompletely measured topic must be marked incomplete so its partial lag is withheld "+
			"from the gauge the threshold rule reads")
	assert.Equal(t, 1, sample.UnmeasuredPartitions,
		"and the count of what could not be read must be published in its place")
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

			_, err = admin.TopicEndOffsets(ctx, time.Time{})
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
			_, err := admin.TopicEndOffsets(ctx, time.Time{})

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

	// THE LAG GAUGE IS ASYNCHRONOUS, so this file renders inventory entries rather than
	// writing the instrument.
	//
	// blnk.kafka.consumer_lag is an Int64ObservableGauge whose callback reads a published
	// inventory once per tick. That indirection is what makes a STALE series clearable: a
	// synchronous Add or Record leaves the last value standing for ever once a subscriber
	// stops being measured, and an alert firing about a subscriber that no longer exists is
	// one nothing can resolve. So the measurement path publishes a complete inventory and the
	// callback observes it, which means the whole set is replaced on every tick and a series
	// that is no longer in it simply stops.
	//
	// This file therefore references ConsumerLagSample and never the instrument. Writing the
	// instrument from here would be a second writer to the same measurement, and two writers
	// make the gauge disagree with itself between ticks.
	assert.Contains(t, referenced, "ConsumerLagSample",
		"the lag figure must be rendered as an entry for the asynchronous gauge's inventory")
	assert.NotContains(t, referenced, "SubscriberConsumerLag",
		"the instrument itself is observed by the collector's callback; a second writer here would "+
			"make the gauge disagree with itself between ticks")
	assert.NotContains(t, referenced, "PublishConsumerLagInventory",
		"and publishing the inventory is the COLLECTOR's job: this file measures, the collector "+
			"decides when a measurement becomes the observed set")
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

// TestProvisionSubscriberPrincipal_ReportsWhatCompensationActuallyAchieved is the F-13 and
// CLEAN-01 guard at the CALL SITE, which is where the defect lived.
//
// Provisioning writes the SCRAM credential first and the ACL bindings second, so a binding
// failure leaves a credential that AUTHENTICATES with no boundary. Compensation revokes it. The
// bug was not in the compensation but in what the caller then claimed: it set Compensated = true
// and CredentialWritten = false unconditionally, whatever the cleanup had managed. A revocation
// that failed was therefore indistinguishable from one that worked, the result said the broker
// was clean, and the downstream branch that reports a live orphan could never run.
//
// # Why the caller's cancellation is an axis of the table rather than a case of its own
//
// The dominant reason provisioning fails is its own five-second issuance budget expiring, so the
// compensation is busiest exactly when the caller's context is already dead. That is not a
// variation on the contract, it is the condition the contract exists for, and it multiplies
// against every failure shape: a revocation can fail with a live caller or a dead one, and the
// result must read identically in both. So the failure shape and the caller's fate are two
// columns of one table, and every case asserts the same invariants.
//
// This suite is the ONLY statement of that contract in the file. Two earlier tests asserted the
// first two cases separately; see the note where they used to sit.
//
// # What each case must establish, without exception
//
//   - THE PREMISE, unconditionally. The cancelled cases require ctx.Err() to be non-nil after
//     the call, so a case whose cancellation never landed FAILS rather than passing vacuously.
//     The previous version of this test cancelled with `defer cancel()` — after provisioning had
//     returned — and then guarded its only assertion behind `if err != nil && CredentialWritten`,
//     so it asserted nothing at all on every run.
//   - THE REPORT MATCHES THE BROKER. Compensated and CredentialWritten are checked against the
//     fake's own credential store, not merely against each other, so a result that describes a
//     state the broker is not in cannot pass.
//   - THE CLEANUP RAN ON A LIVE, BOUNDED CONTEXT. Every compensating call must have seen a
//     context with no error and a deadline. A cleanup that inherited the caller's cancellation
//     would be refused by the fake exactly as a broker refuses it, so `Compensated` would be
//     false — but the context observation says WHY, which is the difference between diagnosing a
//     detachment regression and diagnosing a broker fault.
func TestProvisionSubscriberPrincipal_ReportsWhatCompensationActuallyAchieved(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	const (
		bindingRefused    = "creating ACL bindings was refused"
		revocationRefused = "deleting the credential was refused"
		removalRefused    = "removing the ACL bindings was refused"
	)

	cases := []struct {
		name string
		// cancelCaller cancels the request context from inside the CreateACLs round trip, which
		// is the instant after the credential exists and before provisioning has finished.
		cancelCaller bool
		// scramDeleteError and deleteACLError inject failures into the two compensating calls.
		scramDeleteError error
		deleteACLError   error
		// aclError injects the forward-path failure. The cancelled cases leave it nil, because
		// the cancellation IS their failure — the round trip is refused for the same reason a
		// real broker call is refused when the caller has gone away.
		aclError error

		// wantCompensated and wantCredentialWritten are the two reported flags.
		wantCompensated       bool
		wantCredentialWritten bool
		// wantCause is a substring the returned error must still carry.
		wantCause string
	}{
		{
			name:            "a live caller whose binding fails leaves a clean broker",
			aclError:        errors.New(bindingRefused),
			wantCompensated: true,
			wantCause:       bindingRefused,
		},
		{
			name:                  "a live caller whose revocation also fails is told about the live orphan",
			aclError:              errors.New(bindingRefused),
			scramDeleteError:      errors.New(revocationRefused),
			wantCredentialWritten: true,
			wantCause:             bindingRefused,
		},
		{
			name:            "a caller that goes away mid-provisioning still leaves a clean broker",
			cancelCaller:    true,
			wantCompensated: true,
			wantCause:       context.Canceled.Error(),
		},
		{
			name:                  "a caller that goes away and a revocation that fails is the live orphan",
			cancelCaller:          true,
			scramDeleteError:      errors.New(revocationRefused),
			wantCredentialWritten: true,
			wantCause:             context.Canceled.Error(),
		},
		{
			name: "a caller that goes away and an inert binding left behind is still compensated",
			// A DeleteACLs failure is logged and NOT returned: bindings for a principal that no
			// longer exists grant nothing, so they are untidiness rather than access. The
			// credential revocation is what decides the verdict, and it succeeded.
			cancelCaller:    true,
			deleteACLError:  errors.New(removalRefused),
			wantCompensated: true,
			wantCause:       context.Canceled.Error(),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			fake := newFakeAdminClient()
			if testCase.aclError != nil {
				fake.aclErrors = []error{testCase.aclError}
			}
			fake.scramDeleteError = testCase.scramDeleteError
			if testCase.deleteACLError != nil {
				fake.deleteACLErrors = []error{testCase.deleteACLError}
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if testCase.cancelCaller {
				// CANCELLED FROM INSIDE THE ROUND TRIP, not before the call. Cancelling up front
				// fails the `ready` guard before anything is written, so there is no credential
				// to compensate and the branch under test is never entered — which is why an
				// up-front cancellation cannot test this at all. CreateACLs is chosen because
				// the credential exists by then and the provisioning has not returned.
				fake.onCall = func(method string) {
					if method == "CreateACLs" {
						cancel()
					}
				}
			}

			admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

			result, err := admin.ProvisionSubscriberPrincipal(ctx,
				NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword))

			require.Error(t, err, "the provisioning failure must still be reported to the caller")
			assert.Contains(t, err.Error(), testCase.wantCause,
				"the ORIGINAL cause must survive; replacing it with a cleanup error would hide "+
					"what actually happened")

			// THE PREMISE, REQUIRED RATHER THAN ASSUMED.
			if testCase.cancelCaller {
				require.Error(t, ctx.Err(),
					"this case is about a caller that has gone away, so its context must be dead "+
						"by now; if it is live, the cancellation never landed and everything below "+
						"is asserting the ordinary path under a misleading name")
			} else {
				require.NoError(t, ctx.Err(),
					"this case is about a live caller, so nothing may have cancelled it")
			}

			assert.Equal(t, testCase.wantCompensated, result.Compensated,
				"Compensated must report what the cleanup ACHIEVED")
			assert.Equal(t, testCase.wantCredentialWritten, result.CredentialWritten,
				"CredentialWritten must describe the state the broker is actually left in, not "+
					"the step that ran")
			assert.False(t, result.Compensated && result.CredentialWritten,
				"a live credential and a claimed compensation cannot both be true")

			// THE BROKER ITSELF IS THE ARBITER of the two flags above.
			if testCase.wantCredentialWritten {
				assert.NotEmpty(t, fake.scram,
					"the fixture must confirm the credential really did survive, or the flags "+
						"above are describing a state that never occurred")
			} else {
				assert.Empty(t, fake.scram,
					"the credential written before the failure must not survive it")
			}

			// BOTH COMPENSATING CALLS WERE MADE, AND EACH ON A LIVE, BOUNDED CONTEXT.
			//
			// DeleteACLs is asserted unconditionally because CreateACLs is not atomic across
			// entries: some may have landed before the error, so the removal is attempted
			// whatever the forward path managed.
			assertCompensationRanDetached(t, fake, "DeleteACLs")
			assertCompensationRanDetached(t, fake, "AlterUserScramCredentials")
		})
	}
}

// assertCompensationRanDetached requires that a compensating call was made and that every one of
// its round trips ran on a live, bounded context.
//
// The credential deletion shares its method name with the credential WRITE, so the write's own
// observation is skipped by position: the compensation is whatever came after the forward path,
// and for AlterUserScramCredentials that is every call but the first.
//
// Both halves matter and they fail differently. A cleanup with no deadline is a cleanup that can
// block a request indefinitely on a broker that has gone away, which is why the detached context
// is bounded rather than merely detached. A cleanup with a cancelled context is the CLEAN-01
// defect itself: it returns instantly, revokes nothing, and — before the result was derived from
// the outcome — reported success.
func assertCompensationRanDetached(t *testing.T, fake *fakeAdminClient, method string) {
	t.Helper()

	observations := fake.observedContextsFor(method)

	if method == "AlterUserScramCredentials" {
		require.GreaterOrEqual(t, len(observations), 2,
			"the credential must have been written and then a revocation attempted; with only "+
				"one call there was no compensation at all")
		observations = observations[1:]
	} else {
		require.NotEmpty(t, observations,
			"%s must be attempted during compensation, because CreateACLs is not atomic across "+
				"entries and some bindings may have landed before the failure", method)
	}

	for index, observation := range observations {
		assert.NoErrorf(t, observation.err,
			"compensating %s call %d ran on a context that was already dead, so it could not "+
				"have done anything: the cleanup is inheriting the caller's cancellation instead "+
				"of running on kafkaCleanupContext's detached one", method, index)
		assert.Truef(t, observation.hasDeadline,
			"compensating %s call %d ran with no deadline at all; detached from cancellation is "+
				"not licence to be unbounded, or a refusing broker holds the request open "+
				"indefinitely", method, index)
	}
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

	// ONE PRINCIPAL-WIDE FILTER, not one filter per binding of the current grant.
	//
	// This assertion was inverted. It used to require exactly the five bindings provisioning
	// created, on the reasoning that a broad filter could catch bindings an operator made by
	// hand. That reasoning holds where the principal SURVIVES — compensation unwinding a
	// half-finished provisioning — and fails here, because a subscriber's bindings at the
	// broker are the union of every grant it has ever held while the registry row describes
	// only the latest. Deleting by the current grant walks past every binding an earlier grant
	// left, so a subscriber whose topics were narrowed keeps reading the topic that was
	// removed, with nothing describing the access.
	require.Len(t, deleteRequests[0].Filters, 1,
		"revocation must delete by principal, so bindings from earlier grants cannot survive")

	filter := deleteRequests[0].Filters[0]

	// The principal is the ONLY exact term, and it must carry the "User:" prefix: that is how
	// a principal is stored in a binding, so a filter naming the bare SASL username matches
	// nothing and the delete removes nothing while still reporting success.
	assert.Equal(t, testSubscriberPrincipal(t), filter.PrincipalFilter,
		"the filter must name the principal exactly as a binding stores it")
	assert.True(t, strings.HasPrefix(filter.PrincipalFilter, kafkaPrincipalPrefix),
		"a filter naming the bare SASL username matches nothing and would remove nothing")

	assert.Equal(t, kafka.ResourceTypeAny, filter.ResourceTypeFilter)
	assert.Equal(t, kafka.PatternTypeAny, filter.ResourcePatternTypeFilter)
	assert.Equal(t, kafka.ACLOperationTypeAny, filter.Operation)
	assert.Equal(t, kafka.ACLPermissionTypeAny, filter.PermissionType)

	// Empty rather than "*": these two fields are nullable in the protocol and an empty string
	// encodes as null, which is what Kafka reads as "match any". A literal "*" here would be a
	// resource named "*", which matches nothing.
	assert.Empty(t, filter.ResourceNameFilter,
		"an empty resource name encodes as protocol null, which is the match-any form")
	assert.Empty(t, filter.HostFilter)

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
// ACL reconciliation — narrowing a grant must narrow the enforced boundary
// ---------------------------------------------------------------------------------------

// TestReconcileSubscriberACLs_RevokesRemovedTopicsAndKeepsTheSurvivors is the guard on the
// defect that made a narrowed grant a claim rather than a boundary.
//
// Editing authorized_topics used to touch the registry alone. The broker kept enforcing the
// wider grant, so a subscriber whose topic list was reduced went on reading the removed
// topic with the credential it already held — and the registry, the migration report and
// the isolation criterion all reported the narrower list. Nothing disagreed out loud.
//
// Both halves are asserted, because either alone is wrong: the removed topic's bindings must
// be DELETED, and the surviving topics' bindings must still be in force afterwards.
func TestReconcileSubscriberACLs_RevokesRemovedTopicsAndKeepsTheSurvivors(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	// The row AFTER the narrowing: blnk.balances has been removed from the grant.
	narrowed := testSubscriber()
	narrowed.AuthorizedTopics = []string{"blnk.transactions"}

	require.NoError(t, admin.ReconcileSubscriberACLs(
		context.Background(), narrowed, []string{"blnk.balances"}))

	fake.mu.Lock()
	deleteRequests := fake.deleteACLsRequests
	createRequests := fake.createACLsRequests
	calls := append([]string(nil), fake.calls...)
	fake.mu.Unlock()

	require.Len(t, deleteRequests, 1, "the removed topic's bindings must be deleted exactly once")
	require.Len(t, deleteRequests[0].Filters, 2,
		"Read and Describe on the one removed topic, and nothing else")
	for _, filter := range deleteRequests[0].Filters {
		assert.Equal(t, "blnk.balances", filter.ResourceNameFilter,
			"only the removed topic may be revoked; the surviving grant must be untouched")
		assert.Equal(t, kafka.ResourceTypeTopic, filter.ResourceTypeFilter,
			"the consumer group namespace is derived from identity and survives a topic edit")
	}

	require.Len(t, createRequests, 1, "the desired state must be reapplied")
	survivingTopics := map[string]int{}
	groupBindings := 0
	for _, entry := range createRequests[0].ACLs {
		if entry.ResourceType == kafka.ResourceTypeGroup {
			groupBindings++
			continue
		}
		survivingTopics[entry.ResourceName]++
	}
	assert.Equal(t, map[string]int{"blnk.transactions": 2}, survivingTopics,
		"the surviving topic must keep Read and Describe, and the revoked one must not be recreated")
	assert.Equal(t, 1, groupBindings, "the consumer group namespace must remain granted")

	deleteIndex, createIndex := -1, -1
	for i, call := range calls {
		if call == "DeleteACLs" && deleteIndex < 0 {
			deleteIndex = i
		}
		if call == "CreateACLs" && createIndex < 0 {
			createIndex = i
		}
	}
	require.NotEqual(t, -1, deleteIndex)
	require.NotEqual(t, -1, createIndex)
	assert.Less(t, deleteIndex, createIndex,
		"revocations must be applied first, so a failure over-restricts rather than over-permits")

	fake.mu.Lock()
	scramCalls := len(fake.scramUpsertRequests)
	fake.mu.Unlock()
	assert.Zero(t, scramCalls,
		"reconciling a grant must not touch the SCRAM credential: the subscriber keeps the secret it holds")
}

// TestReconcileSubscriberACLs_ReportsARevocationFailureBeforeReapplying asserts a caller
// cannot be told a narrowing succeeded when the broker refused it.
//
// This is the whole reason revocations go first. If the deletion failed and the desired
// state were reapplied anyway, the call would return success with the removed topic still
// readable — the exact state the reconciliation exists to prevent.
func TestReconcileSubscriberACLs_ReportsARevocationFailureBeforeReapplying(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	fake.transportErrors["DeleteACLs"] = errors.New("not authorized")
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	narrowed := testSubscriber()
	narrowed.AuthorizedTopics = []string{"blnk.transactions"}

	err := admin.ReconcileSubscriberACLs(
		context.Background(), narrowed, []string{"blnk.balances"})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "not authorized")

	fake.mu.Lock()
	createRequests := len(fake.createACLsRequests)
	fake.mu.Unlock()
	assert.Zero(t, createRequests,
		"nothing may be reapplied once a revocation failed; the caller must not read this as done")
}

// TestReconcileSubscriberACLs_EmptyGrantLeavesNothingReadable covers "authorised for
// nothing", which is a legitimate instruction and the strictest possible narrowing.
func TestReconcileSubscriberACLs_EmptyGrantLeavesNothingReadable(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	stripped := testSubscriber()
	stripped.AuthorizedTopics = nil
	stripped.ConsumerGroupID = ""

	require.NoError(t, admin.ReconcileSubscriberACLs(
		context.Background(), stripped, []string{"blnk.transactions", "blnk.balances"}))

	fake.mu.Lock()
	deleteRequests := fake.deleteACLsRequests
	createRequests := len(fake.createACLsRequests)
	fake.mu.Unlock()

	require.Len(t, deleteRequests, 1)
	assert.Len(t, deleteRequests[0].Filters, 4, "Read and Describe on both removed topics")
	assert.Zero(t, createRequests, "an empty grant creates no bindings at all")
}

// TestReconcileSubscriberACLs_RefusesWithoutASubscriber keeps a nil row from becoming a
// broad, undoable deletion.
func TestReconcileSubscriberACLs_RefusesWithoutASubscriber(t *testing.T) {
	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	require.Error(t, admin.ReconcileSubscriberACLs(context.Background(), nil, []string{"blnk.transactions"}))
	assert.Zero(t, fake.totalCalls(), "nothing may be deleted without a row saying what to delete")
}

// ---------------------------------------------------------------------------------------
// Zero-loss reconciliation — bounded per-partition coordinate mapping (V-2)
// ---------------------------------------------------------------------------------------

// auditBound names how a corroborated audit's population is bounded, and therefore which
// broker-side report it may legitimately be reconciled against.
//
// It is a REQUIRED parameter rather than a default because the bound has to match the report:
// the verdict reads WindowRecordCount when both sides name a window and the retained interval
// when neither does, so pairing a windowed audit with an unwindowed report — or the reverse —
// tests the not-windowed caveat instead of whatever the case was written to test, and does so
// without failing. Naming the bound at the call site is what makes that pairing visible.
type auditBound int

const (
	// auditBoundedByRetainedInterval describes an audit bounded by the interval its
	// corroborated rows actually span, which is what the repository reports when the caller
	// asked for the whole retained history. It pairs with measuredReport and with any report
	// that carries partition bounds and no WindowStart.
	auditBoundedByRetainedInterval auditBound = iota

	// auditBoundedBySharedWindow describes an audit bounded by the window BOTH sides were
	// measured from, which is the posture the statistics endpoint uses. It pairs with
	// windowedOffsetReport and with any report carrying reconciliationWindowStart.
	auditBoundedBySharedWindow
)

// corroboratedAudit builds an outbox-side audit in which every published row names a distinct
// broker record.
//
// It is the precondition for a CONCLUSIVE verdict, so cases use it in order to test one
// property at a time rather than accidentally testing the uncorroborated-row caveats: with
// PublishedRows, CorroboratedRows and DistinctCorroboratedRecords all equal there is no
// unconfirmed row, no duplicate coordinate and nothing unmeasured for a caveat to fire on.
//
// # Why there is one of these and not two
//
// There were two — fullyCorroboratedAudit and fullyConfirmedAudit — differing only in the
// bound, with near-identical doc comments each explaining the same three-equal-counts invariant
// in its own words. Nothing named the difference, so
// TestReconcileAgainstOutbox_RefusesToConcludeFromAnIncompleteMeasurement used BOTH inside one
// function, and a reader had no way to tell whether that was deliberate pairing or a copy/paste
// that happened to work. The invariant now lives in one place and the difference is a named
// argument, so a mismatched pairing is visible in the case rather than buried in a helper name.
//
// Parameters:
//   - publishedRows int64: the population size; every row is corroborated.
//   - bound auditBound: which report shape this audit may be reconciled against.
//
// Returns:
//   - model.EventRecordIntervalAudit: an audit with no caveat-bearing row of any kind.
func corroboratedAudit(publishedRows int64, bound auditBound) model.EventRecordIntervalAudit {
	now := time.Now().UTC()

	audit := model.EventRecordIntervalAudit{
		PublishedRows:               publishedRows,
		CorroboratedRows:            publishedRows,
		DistinctCorroboratedRecords: publishedRows,
		MeasuredAt:                  now,
	}

	switch bound {
	case auditBoundedByRetainedInterval:
		// The interval the corroborated rows span. OldestTerminalAt matches its start, because
		// a terminal row older than anything corroborated is exactly the shape that makes a
		// verdict inconclusive and no case here is testing that.
		audit.OldestTerminalAt = now.Add(-time.Hour)
		audit.CorroboratedFrom = now.Add(-time.Hour)
		audit.CorroboratedTo = now

	case auditBoundedBySharedWindow:
		// Fixed rather than time.Now(), so this side and the report name the SAME instant —
		// which is the property the verdict checks before it agrees to compare window totals.
		audit.WindowStart = reconciliationWindowStart()
	}

	return audit
}

// measuredReport builds a broker-side report with one topic and one measured partition,
// so a test can state a window rather than a total.
//
// The partition detail is not decoration. Since the reconciliation verifies each claimed
// coordinate against the live bounds of the partition it names, a report carrying only a summed
// end offset supports no verification at all — and a verdict with nothing verified is back to
// the arithmetic in which a surplus and a compensated loss are indistinguishable. So every case
// states a real interval, which is what a real broker read produces. A non-zero first offset is
// an aged-out window rather than an empty one, and that distinction is the whole point of
// passing the bound rather than a count.
func measuredReport(topic string, partition int, first, end int64) TopicOffsetReport {
	return TopicOffsetReport{
		Topics: []TopicOffsetSnapshot{{
			Topic: topic,
			Partitions: []PartitionOffsetSnapshot{{
				Partition: partition, FirstOffset: first, EndOffset: end,
			}},
			EndOffsetSum:  end,
			RetainedCount: end - first,
		}},
		EndOffsetSum:  end,
		RetainedCount: end - first,
		MeasuredAt:    time.Now().UTC(),
	}
}

// TestTopicOffsetReport_PartitionIntervalsAreTheWindowsTheAuditIsTakenAgainst pins the
// projection the whole bounded reconciliation depends on.
//
// The audit classifies each row's stored coordinate against these windows, so what this
// method emits IS the scope of the verdict. Two properties matter and both are breakable:
// every available partition must appear with its own bounds, and an UNAVAILABLE one must not
// appear at all — emitting it with zeroed bounds would make every row on that partition look
// like it named an offset beyond the log end, reporting a topic recreation where the truth is
// only that the broker did not answer.
//
// The projection is exercised here against stated reports, because every shape a broker can
// produce — an unavailable partition, an aged-out window, a topic missing altogether — is
// reachable that way and only some of them are reachable against a live cluster. That the
// intervals a REAL measurement emits are the intervals a REAL audit is then taken against is
// proven end to end by TestZeroLoss_TheOutboxCensusAndTheBrokerOffsetsReconcileOverRealRecords
// and TestZeroLoss_TheStatisticsProjectionIsAssembledFromLivePostgresAndLiveKafka, which drive
// this method through the production orchestration against a live broker and a live database.
func TestTopicOffsetReport_PartitionIntervalsAreTheWindowsTheAuditIsTakenAgainst(t *testing.T) {
	report := TopicOffsetReport{
		Topics: []TopicOffsetSnapshot{
			{
				Topic: "blnk.transactions",
				Partitions: []PartitionOffsetSnapshot{
					{Partition: 0, FirstOffset: 0, EndOffset: 500},
					{Partition: 1, FirstOffset: 120, EndOffset: 640},
					{Partition: 2, Unavailable: true},
				},
			},
			{
				Topic: "blnk.balances",
				Partitions: []PartitionOffsetSnapshot{
					{Partition: 0, FirstOffset: 9_000, EndOffset: 9_000},
				},
			},
		},
		MeasuredAt: time.Now().UTC(),
	}

	intervals := report.PartitionIntervals()

	assert.Equal(t, []model.PartitionOffsetInterval{
		{Topic: "blnk.transactions", Partition: 0, FirstOffset: 0, EndOffset: 500},
		{Topic: "blnk.transactions", Partition: 1, FirstOffset: 120, EndOffset: 640},
		{Topic: "blnk.balances", Partition: 0, FirstOffset: 9_000, EndOffset: 9_000},
	}, intervals,
		"each available partition contributes its OWN window; a per-topic total could not "+
			"place a coordinate at all")

	for _, interval := range intervals {
		assert.NotEqual(t, 2, interval.Partition,
			"a partition the broker could not report must be ABSENT rather than emitted with "+
				"zeroed bounds, which would misread every row on it as beyond the log end")
	}

	t.Run("a fully aged-out partition is still a measured window", func(t *testing.T) {
		// first == end is a real reading: a partition every record of which has been deleted
		// by retention. It must be reported, because a row naming an offset below it is
		// aged out — a fact — while omitting the window would report the same row as
		// unmeasured, which is a different and weaker statement.
		require.Len(t, intervals, 3)
		assert.Zero(t, intervals[2].Records())
	})

	t.Run("nothing measured yields no windows", func(t *testing.T) {
		assert.Empty(t, TopicOffsetReport{}.PartitionIntervals())
	})
}

// TestReconcileAgainstOutbox_DoesNotRestTheVerdictOnWholeTopicTotals is the guard on the
// finding this reconciliation was rebuilt for.
//
// # The defect
//
// The verdict used to be `endOffsetSum - terminalRows >= 0`. Summed end offsets count RECORDS
// on the whole topic — every redelivery, every replay, every dead-letter copy, and every
// record any OTHER producer ever wrote to a shared topic — while the outbox counts the EVENTS
// it currently retains. The two share no baseline, no readability guarantee, no topic
// incarnation and no producer, so the subtraction was not a weak signal, it was a comparison
// of unrelated quantities that reported itself as conclusive.
//
// # The property
//
// The totals no longer decide anything. A topic carrying a million foreign records reconciles
// exactly as a quiet one does, and a topic whose totals look perfect is INCONCLUSIVE the
// moment a single row cannot be placed in a measured window.
func TestReconcileAgainstOutbox_DoesNotRestTheVerdictOnWholeTopicTotals(t *testing.T) {
	t.Run("a vast surplus of foreign traffic changes nothing", func(t *testing.T) {
		// A shared topic holding a million records, 1,000 of which are Blnk's. Under the old
		// arithmetic the "overhead" was 999,000 and read as health; it is now simply context.
		report := measuredReport("blnk.transactions", 0, 0, 1_000_000)

		verdict := ReconcileAgainstOutbox(report, corroboratedAudit(1_000, auditBoundedByRetainedInterval))

		assert.True(t, verdict.Conclusive,
			"every claim was placed in a measured window, which is what the verdict rests on")
		assert.False(t, verdict.LossDetected)
		assert.Equal(t, int64(1_000_000), verdict.MessagesWritten,
			"the total is still reported, as context")
		assert.Equal(t, int64(1_000), verdict.BlnkRecordShare,
			"the response must state how much of a shared topic's traffic the verdict accounts for")
		assert.Contains(t, verdict.Summary(), "NO LOSS DETECTED")
		assert.Contains(t, verdict.Summary(), "inside the measured offset window",
			"a green verdict must say what it checked, not merely that nothing was detected")
	})

	t.Run("fewer records than rows is no longer a verdict on its own", func(t *testing.T) {
		// The old check called this loss. It is not: on a topic Blnk shares, or one whose
		// records have partly aged out, the total says nothing about whether THIS outbox's
		// records are present. What decides it is whether each row's coordinate is inside a
		// window — and here every one is.
		report := measuredReport("blnk.transactions", 0, 0, 900)

		verdict := ReconcileAgainstOutbox(report, corroboratedAudit(1_000, auditBoundedByRetainedInterval))

		assert.False(t, verdict.LossDetected,
			"a shortfall in whole-topic totals is not evidence about this outbox's own records")
		assert.True(t, verdict.Conclusive)
	})

	t.Run("an empty outbox is conclusive rather than suspicious", func(t *testing.T) {
		// A fresh deployment has published nothing, and a reconciliation that reported that
		// as a problem would fire on every new install.
		verdict := ReconcileAgainstOutbox(
			measuredReport("blnk.transactions", 0, 0, 0),
			model.EventRecordIntervalAudit{},
		)

		assert.True(t, verdict.Conclusive)
		assert.False(t, verdict.LossDetected)
		assert.Zero(t, verdict.CorroboratedEvents)
		assert.Contains(t, verdict.Summary(), "nothing to reconcile",
			"the vacuous case must read as vacuous rather than borrowing the green sentence's "+
				"claim about a window it does not have")
	})
}

// TestReconcileAgainstOutbox_TreatsAnOffsetBeyondTheLogEndAsLoss is the topic-recreation and
// truncation guard.
//
// It is the ONE unambiguous signal in the whole reconciliation. The broker assigned that
// offset when it accepted the write, so the log reached it once; if the end offset is now at
// or below it, the log has been truncated or the topic deleted and recreated, and the records
// those rows name are gone. Nothing else — retention, redelivery, foreign traffic — can
// produce that reading, which is why it sets LossDetected rather than merely a caveat.
func TestReconcileAgainstOutbox_TreatsAnOffsetBeyondTheLogEndAsLoss(t *testing.T) {
	report := measuredReport("blnk.transactions", 0, 0, 40)
	audit := model.EventRecordIntervalAudit{
		PublishedRows:               1_000,
		CorroboratedRows:            960,
		DistinctCorroboratedRecords: 960,
		BeyondEndRows:               40,
		CorroboratedFrom:            time.Now().UTC().Add(-time.Hour),
		CorroboratedTo:              time.Now().UTC(),
	}

	verdict := ReconcileAgainstOutbox(report, audit)

	assert.True(t, verdict.LossDetected,
		"40 rows name records the log can no longer reach; those records are gone")
	assert.False(t, verdict.Conclusive)
	assert.Equal(t, int64(40), verdict.BeyondEndEvents)
	assert.Contains(t, verdict.Summary(), "LOSS DETECTED")
	assert.Contains(t, strings.Join(verdict.Caveats, " "), "truncated or the topic was deleted",
		"the caveat must name the cause, because the remedy for a recreated topic is nothing "+
			"like the remedy for a slow relay")
	assert.Contains(t, verdict.Summary(), "truncated",
		"loss takes precedence in the summary, and it must say which kind of loss")
}

// TestReconcileAgainstOutbox_RefusesToConcludeFromAnIncompleteMeasurement covers the caveats,
// which are what stop a green verdict being reported from a measurement that never covered
// everything.
func TestReconcileAgainstOutbox_RefusesToConcludeFromAnIncompleteMeasurement(t *testing.T) {
	corroborated := corroboratedAudit(1_000, auditBoundedByRetainedInterval)

	cases := map[string]struct {
		report TopicOffsetReport
		audit  model.EventRecordIntervalAudit
		expect string
	}{
		"a missing topic": {
			report: TopicOffsetReport{
				Topics:        measuredReport("blnk.transactions", 0, 0, 1_000).Topics,
				EndOffsetSum:  1_000,
				RetainedCount: 1_000,
				MissingTopics: []string{"blnk.identities"},
				MeasuredAt:    time.Now().UTC(),
			},
			audit:  corroborated,
			expect: "do not exist on the broker",
		},
		"an unavailable partition": {
			report: TopicOffsetReport{
				Topics:                measuredReport("blnk.transactions", 0, 0, 1_000).Topics,
				EndOffsetSum:          1_000,
				RetainedCount:         1_000,
				PartitionsUnavailable: 2,
				MeasuredAt:            time.Now().UTC(),
			},
			audit:  corroborated,
			expect: "did not report offsets",
		},
		"rows on a partition nothing measured": {
			report: measuredReport("blnk.transactions", 0, 0, 1_000),
			audit: model.EventRecordIntervalAudit{
				PublishedRows:               1_000,
				CorroboratedRows:            940,
				DistinctCorroboratedRecords: 940,
				UnmeasuredRows:              60,
			},
			expect: "did not cover",
		},
		"records retention has already deleted": {
			report: measuredReport("blnk.transactions", 0, 400, 1_000),
			audit: model.EventRecordIntervalAudit{
				PublishedRows:               1_000,
				CorroboratedRows:            600,
				DistinctCorroboratedRecords: 600,
				AgedOutRows:                 400,
			},
			expect: "retention has already deleted",
		},
	}

	for name, testCase := range cases {
		t.Run(name, func(t *testing.T) {
			verdict := ReconcileAgainstOutbox(testCase.report, testCase.audit)

			assert.False(t, verdict.Conclusive, "%s leaves claims unaccounted for", name)
			require.NotEmpty(t, verdict.Caveats, "the reason must be stated, not just flagged")
			assert.Contains(t, strings.Join(verdict.Caveats, " "), testCase.expect,
				"each reason needs its OWN wording, or an operator cannot tell a retention "+
					"problem from an unreachable broker")
			assert.Contains(t, verdict.Summary(), "INCONCLUSIVE")
			assert.False(t, verdict.LossDetected,
				"an incomplete measurement is not evidence of loss; only an offset beyond the "+
					"log end is")
		})
	}

	t.Run("aged-out rows are counted as events, not as a topic-wide subtraction", func(t *testing.T) {
		// The caveat this replaces fired whenever ANY record had aged out of a shared topic,
		// including records Blnk never wrote, so it was permanently on in any long-lived
		// deployment and told an operator nothing about their own events. Retention on the
		// topic with none of Blnk's own records affected must now be conclusive.
		report := measuredReport("blnk.transactions", 0, 900_000, 1_000_000)

		verdict := ReconcileAgainstOutbox(report, corroboratedAudit(1_000, auditBoundedByRetainedInterval))

		assert.True(t, verdict.Conclusive,
			"retention that deleted no record THIS OUTBOX named cannot make its verdict inconclusive")
		assert.Zero(t, verdict.AgedOutEvents)
		assert.Equal(t, int64(100_000), verdict.RecordsRetained,
			"the retained total is still reported as context")
	})

	t.Run("healthy retention outside the window is NOT a caveat", func(t *testing.T) {
		// THE REGRESSION THIS PINS (PERF-P05). RetainedCount below EndOffsetSum is what every
		// cluster with a retention policy looks like: end offsets count deleted records, so a
		// windowed reading is unaffected unless records written INSIDE the window were removed.
		// Treating the general case as a caveat made the verdict permanently inconclusive from
		// the first segment deletion onwards, which is an alert nobody can ever clear.
		report := windowedOffsetReport(1_000)
		report.RetainedCount = 12

		verdict := ReconcileAgainstOutbox(report, corroboratedAudit(1_000, auditBoundedBySharedWindow))

		assert.True(t, verdict.Conclusive,
			"retention that removed records from OUTSIDE the measured window changes nothing about "+
				"a comparison drawn inside it")
		assert.Empty(t, verdict.Caveats)
		assert.Contains(t, verdict.Summary(), "NO LOSS DETECTED")
	})

	t.Run("a windowed comparison counts the records written inside the window", func(t *testing.T) {
		// The figure the verdict reads is WindowRecordCount and not the cumulative sum, which
		// is the whole substance of measuring a common population: a topic that has accepted a
		// million records over its life and 1,010 inside the window reconciles against the
		// 1,000 rows the outbox holds for that window, not against the million.
		report := TopicOffsetReport{
			EndOffsetSum:      1_000_000,
			RetainedCount:     1_000_000,
			WindowRecordCount: 1_010,
			WindowStart:       reconciliationWindowStart(),
			MeasuredAt:        time.Now().UTC(),
		}

		verdict := ReconcileAgainstOutbox(report, corroboratedAudit(1_000, auditBoundedBySharedWindow))

		assert.Equal(t, int64(1_010), verdict.MessagesWritten,
			"the cumulative sum counts a history the outbox no longer holds, and comparing against "+
				"it manufactures a surplus that grows without bound")
		assert.Equal(t, int64(10), verdict.Overhead)
		assert.True(t, verdict.Conclusive)
		assert.Equal(t, reconciliationWindowStart(), verdict.WindowStart,
			"the verdict must REPORT the window it was drawn over")
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

	assert.Equal(t, 1, fake.authorizerProbeCount(),
		"the authorizer probe must be answered from the memo after the first ask; it re-learns a fact fixed at broker startup")

	// Past the TTL the question is asked again, so a cluster restarted with a different
	// authorizer setting is noticed rather than reported from a stale answer.
	current = current.Add(defaultOffsetSnapshotTTL + time.Second)

	_, err := admin.ProvisionSubscriberPrincipal(context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword))
	require.NoError(t, err)

	assert.Equal(t, 2, fake.authorizerProbeCount(),
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

	assert.Equal(t, 2, fake.authorizerProbeCount(),
		"a failed probe must be re-asked, not remembered: caching it would turn one bad answer into TTL-long silence about whether the isolation guarantee holds")
	assert.Zero(t, fake.callCount("AlterUserScramCredentials"),
		"no credential may be written while the boundary is unverified")
}

// coherentLagShape renders the lag-bearing content of a report, excluding everything that
// legitimately differs between callers.
//
// Identity and MeasuredAt are dropped because two callers asking about the same topics at the
// same instant SHOULD differ in those. Everything else must be identical, and every input to the
// arithmetic is included rather than just the total: a caller served a partition list from one
// view of the cluster and offset bounds from another produces a plausible total from an
// incoherent snapshot, and only the per-partition detail exposes it.
//
// Parameters:
//   - report ConsumerLagReport: the measurement to project.
//
// Returns:
//   - string: a stable rendering, suitable for equality assertions and readable when one fails.
func coherentLagShape(report ConsumerLagReport) string {
	shape := fmt.Sprintf("total=%d missing=%v", report.TotalLag, report.MissingTopics)

	for _, topic := range report.Topics {
		shape += fmt.Sprintf(" | %s total=%d nocommit=%d unavailable=%d",
			topic.Topic, topic.TotalLag, topic.PartitionsWithoutCommit, topic.PartitionsUnavailable)

		for _, partition := range topic.Partitions {
			shape += fmt.Sprintf(" p%d(committed=%d/%t first=%d end=%d lag=%d)",
				partition.Partition, partition.CommittedOffset, partition.Committed,
				partition.FirstOffset, partition.EndOffset, partition.Lag)
		}
	}

	return shape
}

// TestKafkaAdminClient_SharedCachesAreSafeAndCoherentUnderConcurrentUse drives the two
// process-shared caches from many goroutines at once.
//
// # Why this test has to exist
//
// One KafkaAdminClient is built per process and shared: the credential-issuance endpoint, the
// metrics sweep and the topic assurance pass all reach the same instance, and the whole point of
// the caches is that concurrent callers reuse each other's answers. Every guarantee the caches
// make is therefore a guarantee about CONCURRENT use, and nothing exercised them concurrently.
// The fake is mutex-guarded specifically so `go test -race` can, and no test asked it to.
//
// Three classes of defect are invisible to a serial test and live here:
//
//   - A DATA RACE on the memo fields. cacheMu guards them today; a future path that reads
//     a.authorizerProbe or a.offsetSnapshots without it is a race the race detector reports only
//     if two goroutines actually touch it at once.
//   - AN INCOHERENT SNAPSHOT. The partition layout and the end offsets are cached TOGETHER
//     because a lag computed from two different views of the cluster is a plausible number that
//     is simply wrong. Under concurrency, a cache that stored the two halves separately — or
//     published a half-built snapshot — yields exactly that, and only differing per-caller
//     results reveal it.
//   - A MEMO THAT DOES NOT MEMOISE. Serial tests prove the second call is served from the memo.
//     They cannot show that forty concurrent calls do not each make their own round trip, which
//     is the case the memo exists for: a metrics sweep and an issuance arriving together.
//
// # Why the bounds are inequalities
//
// authorizerActiveCached and partitionOffsetSnapshot check the cache, miss, and then do the
// work: two callers arriving in the same instant can both miss and both ask. That is BENIGN —
// the questions are idempotent and the second answer overwrites the first with the same value —
// so the contract is not "exactly one round trip" but "at most one per concurrent first-arrival,
// and never one per call". Asserting exactly one would be asserting single-flight deduplication
// that the implementation does not claim and does not need. The bounds below are still decisive:
// with the memo removed, every count becomes the call count, which is far above the worker count.
//
// # What is deliberately NOT driven concurrently
//
// InvalidateOffsetSnapshot is, because EnsureTopics calls it after creating or growing a topic
// while a metrics sweep may be in flight — a real interleaving. WithOffsetSnapshotTTL is not: it
// is a fluent configurator with no production caller at all, applied to a client before it is
// shared, and its own documentation says the negative value is for tests rather than
// configuration. Driving it against live readers would report a race on a sequence the type does
// not support, which would be a finding about the test rather than about the client.
func TestKafkaAdminClient_SharedCachesAreSafeAndCoherentUnderConcurrentUse(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	const (
		workers = 8
		rounds  = 5
	)

	t.Run("concurrent callers share the memoised answers", func(t *testing.T) {
		fake := newFakeAdminClient()
		fake.withOffsets("blnk.transactions", 0, 0, 100).withCommitted("blnk.transactions", 0, 40)
		fake.withOffsets("blnk.transactions", 1, 10, 70).withCommitted("blnk.transactions", 1, 55)

		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		// A FROZEN CLOCK, set before any goroutine starts. The TTL must not expire part-way
		// through, or the round-trip bounds below would depend on how long the storm took.
		frozen := time.Now()
		admin.now = func() time.Time { return frozen }

		// Subscribers are derived on THIS goroutine, because derivedSubscriber uses require and
		// a failed require inside a worker goroutine is undefined behaviour rather than a
		// failure. Distinct principals per worker keep the provisionings independent: the same
		// principal provisioned concurrently would report CredentialReplaced for whichever
		// arrived second, which is a property of the fixture rather than of the cache.
		subscribers := make([]*model.EventSubscriber, workers)
		for worker := range subscribers {
			subscribers[worker] = derivedSubscriber(t,
				fmt.Sprintf("cache-race-%02d", worker),
				fmt.Sprintf("Cache Race %02d", worker),
				[]string{"blnk.transactions"},
			)
		}

		start := make(chan struct{})
		shapes := make([]string, workers*rounds)
		failures := make([]error, workers*rounds)
		var group sync.WaitGroup

		for worker := 0; worker < workers; worker++ {
			group.Add(1)

			go func(worker int) {
				defer group.Done()

				// ONE BARRIER, closed once, so every worker leaves at the same instant. Signalling
				// each worker separately would serialise the arrivals and the contention this
				// test exists to create would never happen.
				<-start

				for round := 0; round < rounds; round++ {
					slot := worker*rounds + round

					// The two paths ALTERNATE per round rather than being split across workers,
					// so provisioning and lag reads interleave on every worker and both caches
					// are contended for the whole storm rather than in two phases.
					if round%2 == 0 {
						_, err := admin.ProvisionSubscriberPrincipal(context.Background(),
							NewSubscriberProvisioningRequest(subscribers[worker], sentinelPassword))
						failures[slot] = err

						continue
					}

					report, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{
						SubscriberID: subscribers[worker].SubscriberID,
						GroupID:      subscribers[worker].ConsumerGroupID,
						Topics:       []string{"blnk.transactions"},
					})
					failures[slot] = err
					shapes[slot] = coherentLagShape(report)
				}
			}(worker)
		}

		close(start)
		group.Wait()

		for slot, err := range failures {
			require.NoErrorf(t, err, "slot %d failed; every call here is against a healthy fake", slot)
		}

		// EVERY LAG READING IS THE SAME READING. The fake's state never changes, so any
		// difference is the cache serving one caller a view it did not serve another.
		expected := "total=75 missing=[] | blnk.transactions total=75 nocommit=0 unavailable=0 " +
			"p0(committed=40/true first=0 end=100 lag=60) " +
			"p1(committed=55/true first=10 end=70 lag=15)"
		measured := 0
		for slot, shape := range shapes {
			if shape == "" {
				continue
			}
			measured++
			assert.Equalf(t, expected, shape,
				"slot %d was served a different view of the cluster from its peers", slot)
		}
		require.Positive(t, measured, "no lag reading was taken, so nothing above was asserted")

		// THE MEMOISATION ITSELF.
		lagCalls := measured
		provisionings := workers*rounds - measured

		probes := fake.authorizerProbeCount()
		assert.LessOrEqual(t, probes, workers,
			"the authorizer probe may be asked once per concurrent first-arrival at most; more "+
				"than that means the memo is not being consulted")
		assert.Lessf(t, probes, provisionings,
			"%d provisionings made %d probes: the memo must serve the ones that arrived after an "+
				"answer was cached, or a five-second issuance budget pays for a fixed fact every time",
			provisionings, probes)

		layoutReads := fake.callCount("Metadata")
		boundsReads := fake.callCount("ListOffsets")
		assert.LessOrEqual(t, layoutReads, workers,
			"the partition layout is a property of the topics; one read per concurrent "+
				"first-arrival is the ceiling")
		assert.LessOrEqual(t, boundsReads, workers,
			"the end offsets are a property of the topics too")
		assert.Lessf(t, layoutReads, lagCalls,
			"%d lag reads made %d layout reads; the snapshot must be shared or the metrics sweep "+
				"is back to 3N round trips", lagCalls, layoutReads)
		assert.Equal(t, layoutReads, boundsReads,
			"the layout and the bounds are ONE snapshot, so they are read the same number of "+
				"times; a divergence means the two halves are being cached separately, which is "+
				"how a lag comes to be computed from two views of the cluster")

		// The committed offsets are the one question that genuinely differs per group, so they
		// are NOT cached and must be asked exactly once per lag reading.
		assert.Equal(t, lagCalls, fake.callCount("OffsetFetch"),
			"a cached committed offset would report another group's position as this group's")
	})

	t.Run("an invalidation racing readers never yields an incoherent answer", func(t *testing.T) {
		// EnsureTopics invalidates after creating or growing a topic, so an invalidation arriving
		// while a metrics sweep is mid-flight is an ordinary production interleaving rather than a
		// contrived one. Round-trip counts are deliberately NOT asserted here: an invalidation
		// forces a re-read by design, so the count is a function of the scheduler. What must hold
		// regardless is that no caller ever sees a PARTIAL snapshot — a partition present in the
		// layout but absent from the bounds, or a set of bounds from before an invalidation paired
		// with a layout from after it.
		fake := newFakeAdminClient()
		fake.withOffsets("blnk.transactions", 0, 0, 100).withCommitted("blnk.transactions", 0, 40)
		fake.withOffsets("blnk.transactions", 1, 10, 70).withCommitted("blnk.transactions", 1, 55)

		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		start := make(chan struct{})
		stop := make(chan struct{})
		shapes := make([]string, workers*rounds)
		failures := make([]error, workers*rounds)

		var readers, invalidators sync.WaitGroup

		// The invalidator runs for exactly as long as the readers do — stopped only once every
		// reader has finished — so it is contending for the whole of their lifetime rather than
		// for a fixed interval that may or may not overlap them.
		invalidators.Add(1)
		go func() {
			defer invalidators.Done()

			<-start

			for {
				select {
				case <-stop:
					return
				default:
					admin.InvalidateOffsetSnapshot()
					// Yielded explicitly: an unyielding loop on a busy machine can hold the
					// processor long enough that the readers make no progress, which would leave
					// the interleaving this subtest depends on untested.
					runtime.Gosched()
				}
			}
		}()

		for worker := 0; worker < workers; worker++ {
			readers.Add(1)

			go func(worker int) {
				defer readers.Done()

				<-start

				for round := 0; round < rounds; round++ {
					slot := worker*rounds + round

					report, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{
						SubscriberID: fmt.Sprintf("invalidation-%02d", worker),
						GroupID:      fmt.Sprintf("blnk-sub-invalidation-%02d", worker),
						Topics:       []string{"blnk.transactions"},
					})
					failures[slot] = err
					shapes[slot] = coherentLagShape(report)
				}
			}(worker)
		}

		close(start)
		readers.Wait()
		close(stop)
		invalidators.Wait()

		expected := "total=75 missing=[] | blnk.transactions total=75 nocommit=0 unavailable=0 " +
			"p0(committed=40/true first=0 end=100 lag=60) " +
			"p1(committed=55/true first=10 end=70 lag=15)"

		for slot, err := range failures {
			require.NoErrorf(t, err, "slot %d failed while a snapshot was being invalidated; an "+
				"invalidation must cost a round trip, never an error", slot)
			assert.Equalf(t, expected, shapes[slot],
				"slot %d was served an incoherent view while the snapshot was being invalidated",
				slot)
		}

		assert.Positive(t, fake.callCount("Metadata"),
			"the readers must actually have reached the broker, or the invalidation raced nothing")
	})
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

// ---------------------------------------------------------------------------------------
// ACL reconciliation — AUTH-02
// ---------------------------------------------------------------------------------------

// aclKeys renders a set of bindings as sorted identity tuples, so two sets can be compared as
// sets rather than as ordered slices.
func aclKeys(entries []kafka.ACLEntry) []string {
	keys := make([]string, 0, len(entries))
	for _, entry := range entries {
		keys = append(keys, fakeACLKey(entry))
	}
	sort.Strings(keys)

	return keys
}

// topicBinding builds one Blnk-shaped topic binding for the fixture principal.
func topicBinding(t *testing.T, topic string, operation kafka.ACLOperationType) kafka.ACLEntry {
	t.Helper()

	return kafka.ACLEntry{
		ResourceType:        kafka.ResourceTypeTopic,
		ResourceName:        topic,
		ResourcePatternType: kafka.PatternTypeLiteral,
		Principal:           testSubscriberPrincipal(t),
		Host:                "*",
		Operation:           operation,
		PermissionType:      kafka.ACLPermissionTypeAllow,
	}
}

// TestReconcileSubscriberACLs_RemovesTheGrantOfAPreviousAuthorization is the AUTH-02 guard.
//
// Provisioning used to CREATE bindings and never remove any, so a subscriber's broker-side
// grant was the UNION of every authorization it had ever held. Narrowing authorized_topics
// updated the registry and left the removed topic's Read and Describe bindings live: the
// registry said the access was gone, the subscriber kept consuming, and no request failed to
// say otherwise. Revocation could not clean it up either, because it derives what to delete
// from the CURRENT row — the row that no longer names the topic.
//
// The property asserted is CONVERGENCE, not merely that a delete was sent: after
// reconciliation the broker's Blnk-owned bindings for the principal must equal the desired set
// exactly, with the surplus gone and the missing created.
func TestReconcileSubscriberACLs_RemovesTheGrantOfAPreviousAuthorization(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	// The broker starts from the WIDER authorization: the subscriber was once granted
	// blnk.identities as well, and those two bindings are the residue.
	wide := testSubscriber()
	wide.AuthorizedTopics = []string{"blnk.transactions", "blnk.balances", "blnk.identities"}

	_, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(wide, sentinelPassword),
	)
	require.NoError(t, err)
	require.Len(t, fake.heldBindings(), 7, "three topics is six topic bindings plus the group binding")

	// Now the authorization is narrowed to two topics, which is what an authorization update
	// records, and provisioning runs again.
	narrow := testSubscriber()
	narrow.AuthorizedTopics = []string{"blnk.transactions", "blnk.balances"}

	result, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(narrow, sentinelPassword),
	)
	require.NoError(t, err)

	assert.Equal(t, 2, result.ACLBindingsRemoved,
		"the two bindings of the topic that was dropped must be deleted, not merely left unlisted")

	desired, principal, err := subscriberDesiredBindings(narrow)
	require.NoError(t, err)
	require.Equal(t, 5, len(desired))

	assert.Equal(t, aclKeys(desired), aclKeys(fake.heldBindings()),
		"after reconciliation the broker must hold EXACTLY the narrowed grant; a surviving "+
			"blnk.identities binding is live access the registry says was removed")

	for _, held := range fake.heldBindings() {
		assert.NotEqual(t, "blnk.identities", held.ResourceName,
			"the dropped topic must have no binding left on principal "+principal)
	}
}

// TestReconcileSubscriberACLs_ConvergesToNothingWhenTheGrantIsCleared covers the empty desired
// set.
//
// Clearing the topic list is a legitimate instruction and it means "authorised for nothing".
// Treating an empty desired set as "no work to do" is precisely the reading that let a
// narrowing-to-empty leave a full grant standing, so reconciliation must remove every
// Blnk-owned binding instead.
func TestReconcileSubscriberACLs_ConvergesToNothingWhenTheGrantIsCleared(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	subscriber := testSubscriber()
	_, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
	)
	require.NoError(t, err)
	require.Len(t, fake.heldBindings(), 5)

	cleared := testSubscriber()
	cleared.AuthorizedTopics = nil

	report, err := admin.PruneSubscriberAccess(context.Background(), cleared)
	require.NoError(t, err)

	assert.Equal(t, 4, report.Removed,
		"every topic binding must go when the grant is cleared")
	assert.Equal(t, 5, report.Managed)

	// The group binding survives, because the consumer group namespace is derived from the
	// identifier rather than from the topic list: a subscriber authorised for no topic still
	// owns its own group namespace, and revoking that is deregistration's job, not an
	// authorization update's.
	held := fake.heldBindings()
	require.Len(t, held, 1)
	assert.Equal(t, kafka.ResourceTypeGroup, held[0].ResourceType)
}

// deniedTopicBinding builds a foreign DENY binding on a topic: a shape Blnk never provisions
// and which can only NARROW what the grant allows.
func deniedTopicBinding(principal, topic string) kafka.ACLEntry {
	return kafka.ACLEntry{
		ResourceType:        kafka.ResourceTypeTopic,
		ResourceName:        topic,
		ResourcePatternType: kafka.PatternTypeLiteral,
		Principal:           principal,
		Host:                "*",
		Operation:           kafka.ACLOperationTypeRead,
		PermissionType:      kafka.ACLPermissionTypeDeny,
	}
}

// TestReconcileSubscriberACLs_LeavesForeignDenyBindingsAloneAndCarriesOn is the safety half of
// AUTH-02, for the bindings that are safe to leave.
//
// ACL deletion has no undo, and a reconciliation wide enough to tidy an operator's deliberate
// work is wide enough to delete something load-bearing that nobody remembers creating. So the
// ownership test is an ALLOWLIST of the two shapes Blnk provisions, and a foreign binding is
// never deleted.
//
// A DENY is additionally safe to CARRY ON PAST. It subtracts from what the ALLOW bindings
// grant, so its presence means the subscriber can read LESS than its authorization describes —
// which cannot be an isolation failure, and which an operator may well have added on purpose.
// Refusing on one would block credential issuance for a principal that is more restricted than
// Blnk requires.
func TestReconcileSubscriberACLs_LeavesForeignDenyBindingsAloneAndCarriesOn(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	principal := testSubscriberPrincipal(t)

	denies := []kafka.ACLEntry{
		deniedTopicBinding(principal, "blnk.transactions"),
		deniedTopicBinding(principal, "blnk.balances"),
	}

	fake := newFakeAdminClient()
	for _, binding := range denies {
		fake.withBinding(binding)
	}

	// And one genuinely stale Blnk-shaped binding, so the test proves reconciliation still
	// does its job rather than passing by doing nothing at all.
	stale := topicBinding(t, "blnk.identities", kafka.ACLOperationTypeRead)
	fake.withBinding(stale)

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword),
	)
	require.NoError(t, err, "a DENY narrows access and must not block issuance")

	assert.Equal(t, 1, report.ACLBindingsRemoved,
		"exactly the one stale Blnk-shaped binding may be removed")
	assert.Equal(t, len(denies), report.ForeignACLBindings,
		"every binding outside Blnk's ownership must be REPORTED so an operator can decide")
	assert.Zero(t, report.ForeignACLBindingsGranting,
		"a DENY SUBTRACTS from what the ALLOW bindings grant, so none of these grants access — "+
			"which is exactly why the reconciliation may carry on past them")

	// THE ISSUANCE SUCCEEDED, so the credential is at the broker and nothing was compensated.
	//
	// This is the half that distinguishes this test from
	// TestProvisionSubscriberPrincipal_RefusesAForeignAllowBinding below, which asserts the
	// mirror image on the same fields. The require.NoError three lines up is what forces this
	// direction: a call that returned no error refused nothing, so there is no failure for a
	// compensation to undo and the credential must be exactly where a clean issuance leaves it.
	assert.True(t, report.CredentialWritten,
		"a DENY narrows access, so the credential is written exactly as it would be without one")
	assert.False(t, report.Compensated,
		"nothing was undone, because nothing was refused")
	assert.NotEmpty(t, fake.scram,
		"the SCRAM credential must be at the broker: that is what issuance produced")

	held := aclKeys(fake.heldBindings())
	for _, binding := range denies {
		assert.Contains(t, held, fakeACLKey(binding),
			"a binding Blnk does not provision must survive untouched, refusal or not")
	}
	assert.NotContains(t, held, fakeACLKey(stale),
		"the stale Blnk-shaped binding must be gone")
}

// TestProvisionSubscriberPrincipal_RefusesAForeignAllowBinding is AUTH-03, and it is a
// data-disclosure guard rather than a tidiness one.
//
// # What was wrong
//
// A foreign ALLOW binding was DETECTED, logged at warning level, and then provisioning carried
// on: the credential was minted, the password was returned, and the issuance response declared
// an enforced access boundary the broker was not enforcing. The subscriber held whatever the
// foreign binding granted — a wildcard topic pattern, a cluster Describe, another tenant's
// topic — and nothing an integrator could read said so. The only trace was one log line in the
// stream of a SUCCESSFUL request.
//
// # What must hold now
//
// Every one of these four properties is part of the fix, and each is asserted:
//
//  1. The call REFUSES, with an error classifiable as ErrSubscriberForeignACLGrant.
//  2. NOTHING is deleted and nothing is created. The refusal happens before the mutations, so
//     the broker is left exactly as it was found — including the stale Blnk-shaped binding,
//     which is a deliberate trade: converging half a grant on a principal whose effective
//     access cannot be stated is worse than converging none of it.
//  3. The SCRAM credential written moments earlier is COMPENSATED — revoked — so no usable
//     credential survives the refusal.
//  4. The password does not appear in the error.
//
// The four foreign shapes are each independently sufficient, so each is exercised on its own:
// a table would let one passing shape mask a failing one.
func TestProvisionSubscriberPrincipal_RefusesAForeignAllowBinding(t *testing.T) {
	principal := func(t *testing.T) string {
		t.Helper()

		return testSubscriberPrincipal(t)
	}

	cases := map[string]func(t *testing.T) kafka.ACLEntry{
		"a Write an operator granted deliberately": func(t *testing.T) kafka.ACLEntry {
			// Blnk never grants Write, so it cannot have created this — and it lets the
			// subscriber PUBLISH onto a ledger topic, which is the most serious of the four.
			return kafka.ACLEntry{
				ResourceType:        kafka.ResourceTypeTopic,
				ResourceName:        "blnk.transactions",
				ResourcePatternType: kafka.PatternTypeLiteral,
				Principal:           principal(t),
				Host:                "*",
				Operation:           kafka.ACLOperationTypeWrite,
				PermissionType:      kafka.ACLPermissionTypeAllow,
			}
		},
		"a PREFIXED topic pattern": func(t *testing.T) kafka.ACLEntry {
			// Blnk grants topics literally. A prefix of "blnk." grants EVERY Blnk topic,
			// including every other subscriber's and every dead-letter topic.
			return kafka.ACLEntry{
				ResourceType:        kafka.ResourceTypeTopic,
				ResourceName:        "blnk.",
				ResourcePatternType: kafka.PatternTypePrefixed,
				Principal:           principal(t),
				Host:                "*",
				Operation:           kafka.ACLOperationTypeRead,
				PermissionType:      kafka.ACLPermissionTypeAllow,
			}
		},
		"a cluster resource": func(t *testing.T) kafka.ACLEntry {
			// Outside the two resource types Blnk provisions entirely, and cluster Describe
			// lets a subscriber enumerate every topic in the cluster.
			return kafka.ACLEntry{
				ResourceType:        kafka.ResourceTypeCluster,
				ResourceName:        "kafka-cluster",
				ResourcePatternType: kafka.PatternTypeLiteral,
				Principal:           principal(t),
				Host:                "*",
				Operation:           kafka.ACLOperationTypeDescribe,
				PermissionType:      kafka.ACLPermissionTypeAllow,
			}
		},
		"a binding whose permission type is not stated": func(t *testing.T) kafka.ACLEntry {
			// FAIL CLOSED on the unknown. A binding the broker did not describe definitively
			// cannot be shown to narrow anything, and "I could not tell" must never be
			// recorded as "it is safe".
			return kafka.ACLEntry{
				ResourceType:        kafka.ResourceTypeTopic,
				ResourceName:        "blnk.balances",
				ResourcePatternType: kafka.PatternTypeLiteral,
				Principal:           principal(t),
				Host:                "*",
				Operation:           kafka.ACLOperationTypeRead,
				PermissionType:      kafka.ACLPermissionTypeUnknown,
			}
		},
	}

	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			storeKafkaTopicPrefix(t, DefaultTopicPrefix)

			foreign := build(t)

			fake := newFakeAdminClient().withBinding(foreign)

			// A stale Blnk-shaped binding, so the test can prove reconciliation performed NO
			// mutation rather than merely that it had nothing to do.
			stale := topicBinding(t, "blnk.identities", kafka.ACLOperationTypeRead)
			fake.withBinding(stale)

			admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)
			subscriber := testSubscriber()

			result, err := admin.ProvisionSubscriberPrincipal(
				context.Background(),
				NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
			)

			require.Error(t, err, "a widening binding must refuse, not warn")
			assert.ErrorIs(t, err, ErrSubscriberForeignACLGrant,
				"the refusal must be classifiable, so the caller can report the operational remedy")
			assert.NotContains(t, err.Error(), sentinelPassword,
				"no error may carry the generated password")

			held := aclKeys(fake.heldBindings())
			assert.Contains(t, held, fakeACLKey(foreign),
				"the foreign binding is the operator's, and ACL deletion has no undo")
			assert.Contains(t, held, fakeACLKey(stale),
				"the refusal must precede every mutation, so even a stale Blnk binding survives")

			assert.Zero(t, result.ACLBindingsRemoved)
			assert.Zero(t, result.ACLBindings,
				"no binding count may be reported for a provisioning that refused")

			// AUTH-01: the credential was written before the ACL step, so the refusal must
			// leave no usable credential behind.
			assert.True(t, result.Compensated,
				"the SCRAM credential written before the refusal must be revoked")
			assert.False(t, result.CredentialWritten,
				"a confirmed revocation must clear CredentialWritten, so no caller believes a "+
					"credential survives")
		})
	}
}

// TestGrantSubscriberAccess_RefusesAForeignAllowBindingButPruneDoesNot pins the ONE asymmetry
// in the fail-closed rule, which is the difference between a widening and a narrowing.
//
// Both operations run on a principal whose effective access Blnk cannot state. They must answer
// differently, and the direction of travel is why:
//
//   - GRANTING is a widening. Converging it would report the registry and the broker as
//     agreeing when they demonstrably do not, so it refuses — leaving the subscriber with the
//     access it already had, which is LESS than the row records and is the safe direction this
//     whole three-step surface is built around.
//   - PRUNING only ever REMOVES access. Refusing it would leave the subscriber with MORE access
//     than the operator just asked for, which is precisely the outcome the refusal exists to
//     prevent. So it proceeds, removes the obsolete Blnk-owned bindings, and reports the
//     foreign one.
func TestGrantSubscriberAccess_RefusesAForeignAllowBindingButPruneDoesNot(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	principal := testSubscriberPrincipal(t)

	widening := kafka.ACLEntry{
		ResourceType:        kafka.ResourceTypeTopic,
		ResourceName:        "blnk.",
		ResourcePatternType: kafka.PatternTypePrefixed,
		Principal:           principal,
		Host:                "*",
		Operation:           kafka.ACLOperationTypeRead,
		PermissionType:      kafka.ACLPermissionTypeAllow,
	}

	t.Run("granting refuses", func(t *testing.T) {
		fake := newFakeAdminClient().withBinding(widening)
		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		report, err := admin.GrantSubscriberAccess(context.Background(), testSubscriber())

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSubscriberForeignACLGrant)
		assert.Zero(t, report.Created, "nothing may be granted on top of an unknown grant")
		assert.Len(t, report.ForeignAllow, 1)
		assert.Empty(t, report.ForeignDeny)
	})

	t.Run("pruning proceeds and reports", func(t *testing.T) {
		fake := newFakeAdminClient().withBinding(widening)

		// A Blnk-shaped binding for a topic the subscriber is no longer authorised for: the
		// obsolete grant the prune exists to remove.
		stale := topicBinding(t, "blnk.identities", kafka.ACLOperationTypeRead)
		fake.withBinding(stale)

		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		report, err := admin.PruneSubscriberAccess(context.Background(), testSubscriber())

		require.NoError(t, err,
			"refusing a narrowing would leave MORE access than the operator asked for")
		assert.Equal(t, 1, report.Removed, "the obsolete Blnk-owned binding must still go")
		assert.Len(t, report.ForeignAllow, 1,
			"the widening binding must be reported, so the caller and the log say the narrowing "+
				"did not narrow the effective access")

		held := aclKeys(fake.heldBindings())
		assert.Contains(t, held, fakeACLKey(widening))
		assert.NotContains(t, held, fakeACLKey(stale))
	})
}

// TestForeignACLBindingWidens_TreatsOnlyAnExplicitDenyAsHarmless pins the classifier the whole
// refusal turns on.
//
// Kafka's permission type has four values and only ONE of them provably takes access away. The
// other three are treated as widening, which is the fail-closed reading: Unknown and Any are
// values a described binding should never carry, so seeing one means the client or broker
// returned something this code does not understand — exactly when guessing is worst.
func TestForeignACLBindingWidens_TreatsOnlyAnExplicitDenyAsHarmless(t *testing.T) {
	for name, tc := range map[string]struct {
		permission kafka.ACLPermissionType
		widens     bool
	}{
		"allow widens":   {kafka.ACLPermissionTypeAllow, true},
		"deny narrows":   {kafka.ACLPermissionTypeDeny, false},
		"unknown widens": {kafka.ACLPermissionTypeUnknown, true},
		"any widens":     {kafka.ACLPermissionTypeAny, true},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.widens, foreignACLBindingWidens(kafka.ACLEntry{
				ResourceType:   kafka.ResourceTypeTopic,
				PermissionType: tc.permission,
			}))
		})
	}

	t.Run("the two lists are reported separately and combine in order", func(t *testing.T) {
		report := SubscriberACLReconciliation{
			ForeignAllow: []string{"allow-1", "allow-2"},
			ForeignDeny:  []string{"deny-1"},
		}

		assert.Equal(t, []string{"allow-1", "allow-2", "deny-1"}, report.Foreign(),
			"the combined view is for logging and counting, widening first")
		assert.Nil(t, SubscriberACLReconciliation{}.Foreign(),
			"no foreign bindings must yield nil rather than an empty slice a caller has to check")
	})
}

// TestProvisionSubscriberPrincipal_RefusesToOverwriteAReservedPrincipal is SEC-05, and it is a
// privilege-escalation guard.
//
// Provisioning performs a SCRAM UPSERT: an existing credential for the principal is REPLACED
// with a freshly generated password, which the issuance response then returns. So a subscriber
// whose derived principal equals the administrative or producer username does not get a new
// identity — it gets THAT identity's credential rotated and handed to the caller, along with
// either Write on every Blnk-owned topic or the ability to mint credentials and grant ACLs.
//
// Configuration validation refuses such a deployment at start-up. This is the second gate, and
// it is not redundant: configuration can be reloaded, and an admin client can be constructed
// from a configuration this process never validated. It must refuse BEFORE the upsert, so the
// assertion is that no SCRAM write happened at all.
func TestProvisionSubscriberPrincipal_RefusesToOverwriteAReservedPrincipal(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	subscriber := testSubscriber()

	// The BARE SASL username, not the "User:" ACL principal string that
	// testSubscriberPrincipal renders. reservedPrincipals holds the usernames read out of
	// KAFKA_SASL_USER and KAFKA_SASL_ADMIN_USER, and the derived value the SCRAM upsert would
	// write is the bare one — comparing the two forms is exactly the mistake that would make
	// this guard never fire.
	principal, err := model.CanonicalKafkaPrincipal(testSubscriberID)
	require.NoError(t, err)

	for name, reserved := range map[string][]string{
		"the administrative principal": {principal, "blnk-producer"},
		"the producer principal":       {"blnk-admin", principal},
		"both":                         {principal, principal},
	} {
		t.Run(name, func(t *testing.T) {
			fake := newFakeAdminClient()
			admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)
			admin.reservedPrincipals = reserved

			result, err := admin.ProvisionSubscriberPrincipal(
				context.Background(),
				NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
			)

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrSubscriberPrincipalReserved)
			assert.NotContains(t, err.Error(), sentinelPassword)

			assert.False(t, result.CredentialWritten,
				"the refusal must precede the SCRAM upsert")
			assert.Zero(t, fake.callCount("AlterUserScramCredentials"),
				"no credential write may be attempted for a reserved principal")
			assert.Zero(t, fake.callCount("CreateACLs"))
			assert.False(t, result.AuthorizerActive,
				"the refusal must precede even the authorizer probe, so it costs no round trip")
		})
	}

	t.Run("an unrelated reserved principal does not block provisioning", func(t *testing.T) {
		fake := newFakeAdminClient()
		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)
		admin.reservedPrincipals = []string{"blnk-admin", "blnk-producer"}

		_, err := admin.ProvisionSubscriberPrincipal(
			context.Background(),
			NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
		)
		require.NoError(t, err)
	})

	t.Run("comparison is case sensitive, because Kafka principals are", func(t *testing.T) {
		fake := newFakeAdminClient()
		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)
		admin.reservedPrincipals = []string{strings.ToUpper(principal)}

		_, err := admin.ProvisionSubscriberPrincipal(
			context.Background(),
			NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
		)
		require.NoError(t, err,
			"folding case would refuse a provisioning that is in fact safe, and tell the operator "+
				"something untrue about why")
	})
}

// TestReservedKafkaPrincipals_ReadsBothIdentitiesAndDeduplicates pins what the admin client
// carries into the check above.
func TestReservedKafkaPrincipals_ReadsBothIdentitiesAndDeduplicates(t *testing.T) {
	for name, tc := range map[string]struct {
		admin, producer string
		want            []string
	}{
		"both configured and distinct": {"blnk-admin", "blnk-producer", []string{"blnk-admin", "blnk-producer"}},
		"only the administrative one":  {"blnk-admin", "", []string{"blnk-admin"}},
		"only the producer":            {"", "blnk-producer", []string{"blnk-producer"}},
		"neither":                      {"", "", nil},
		"whitespace is trimmed":        {"  blnk-admin  ", "", []string{"blnk-admin"}},
		// Deduplicated so the list reads cleanly. The COLLISION itself is refused at
		// configuration load, not here — this function only reports the identities.
		"the same identity twice": {"blnk-admin", "blnk-admin", []string{"blnk-admin"}},
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.want, reservedKafkaPrincipals(config.KafkaConfig{
				SASLAdminUser: tc.admin,
				SASLUser:      tc.producer,
			}))
		})
	}
}

// TestReservedSubscriberPrincipalNamespace_IsPinnedAcrossPackages is the only thing keeping two
// copies of one constant from drifting.
//
// The config package cannot import model — model already depends on config, and the import
// would close the cycle — so the reserved namespace is restated in config and asserted equal
// here. A drift would be silent and would disable the start-up check for exactly the namespace
// it is meant to protect.
func TestReservedSubscriberPrincipalNamespace_IsPinnedAcrossPackages(t *testing.T) {
	assert.Equal(t, model.SubscriberPrincipalNamespace, config.ReservedSubscriberPrincipalNamespace,
		"config restates the reserved namespace because it cannot import model; the two must be equal")
	assert.Equal(t, "blnk-sub-", model.SubscriberPrincipalNamespace,
		"the namespace is a published contract: changing it orphans every principal already minted")
}

// TestBlnkManagedACLBinding_IsAnAllowlistOfExactlyTwoShapes pins the ownership test itself.
//
// It is the single decision the whole reconciliation rests on: true means "Blnk may delete
// this". Anything wider gives reconciliation permission over an operator's work; anything
// narrower fails to recognise a binding from a previous authorization as Blnk's own, which is
// precisely the binding that has to be removed.
func TestBlnkManagedACLBinding_IsAnAllowlistOfExactlyTwoShapes(t *testing.T) {
	owned := []struct {
		name    string
		binding kafka.ACLEntry
	}{
		{"literal topic Read", kafka.ACLEntry{
			ResourceType: kafka.ResourceTypeTopic, ResourcePatternType: kafka.PatternTypeLiteral,
			Operation: kafka.ACLOperationTypeRead, PermissionType: kafka.ACLPermissionTypeAllow,
		}},
		{"literal topic Describe", kafka.ACLEntry{
			ResourceType: kafka.ResourceTypeTopic, ResourcePatternType: kafka.PatternTypeLiteral,
			Operation: kafka.ACLOperationTypeDescribe, PermissionType: kafka.ACLPermissionTypeAllow,
		}},
		{"prefixed group Read", kafka.ACLEntry{
			ResourceType: kafka.ResourceTypeGroup, ResourcePatternType: kafka.PatternTypePrefixed,
			Operation: kafka.ACLOperationTypeRead, PermissionType: kafka.ACLPermissionTypeAllow,
		}},
	}

	for _, shape := range owned {
		t.Run("owns "+shape.name, func(t *testing.T) {
			assert.True(t, blnkManagedACLBinding(shape.binding),
				"this is a shape aclEntries produces, so reconciliation must be able to remove it")
		})
	}

	disowned := []struct {
		name    string
		binding kafka.ACLEntry
	}{
		{"a deny", kafka.ACLEntry{
			ResourceType: kafka.ResourceTypeTopic, ResourcePatternType: kafka.PatternTypeLiteral,
			Operation: kafka.ACLOperationTypeRead, PermissionType: kafka.ACLPermissionTypeDeny,
		}},
		{"a topic write", kafka.ACLEntry{
			ResourceType: kafka.ResourceTypeTopic, ResourcePatternType: kafka.PatternTypeLiteral,
			Operation: kafka.ACLOperationTypeWrite, PermissionType: kafka.ACLPermissionTypeAllow,
		}},
		{"a prefixed topic", kafka.ACLEntry{
			ResourceType: kafka.ResourceTypeTopic, ResourcePatternType: kafka.PatternTypePrefixed,
			Operation: kafka.ACLOperationTypeRead, PermissionType: kafka.ACLPermissionTypeAllow,
		}},
		{"a literal group", kafka.ACLEntry{
			ResourceType: kafka.ResourceTypeGroup, ResourcePatternType: kafka.PatternTypeLiteral,
			Operation: kafka.ACLOperationTypeRead, PermissionType: kafka.ACLPermissionTypeAllow,
		}},
		{"a group describe", kafka.ACLEntry{
			ResourceType: kafka.ResourceTypeGroup, ResourcePatternType: kafka.PatternTypePrefixed,
			Operation: kafka.ACLOperationTypeDescribe, PermissionType: kafka.ACLPermissionTypeAllow,
		}},
		{"a cluster grant", kafka.ACLEntry{
			ResourceType: kafka.ResourceTypeCluster, ResourcePatternType: kafka.PatternTypeLiteral,
			Operation: kafka.ACLOperationTypeDescribe, PermissionType: kafka.ACLPermissionTypeAllow,
		}},
		{"a transactional id", kafka.ACLEntry{
			ResourceType: kafka.ResourceTypeTransactionalID, ResourcePatternType: kafka.PatternTypeLiteral,
			Operation: kafka.ACLOperationTypeWrite, PermissionType: kafka.ACLPermissionTypeAllow,
		}},
	}

	for _, shape := range disowned {
		t.Run("disowns "+shape.name, func(t *testing.T) {
			assert.False(t, blnkManagedACLBinding(shape.binding),
				"Blnk never provisions this shape, so it must never delete one")
		})
	}
}

// TestACLBindingKey_TreatsHostAsPartOfTheGrant pins binding identity.
//
// A binding that differs only by host is a DIFFERENT grant: it admits a different set of
// clients. Treating the two as the same set member would leave a stale host-scoped grant in
// place while reporting the set converged, which is the same fail-open the reconciliation
// exists to remove.
func TestACLBindingKey_TreatsHostAsPartOfTheGrant(t *testing.T) {
	base := kafka.ACLEntry{
		ResourceType:        kafka.ResourceTypeTopic,
		ResourceName:        "blnk.transactions",
		ResourcePatternType: kafka.PatternTypeLiteral,
		Principal:           "User:blnk-sub-acme",
		Host:                "*",
		Operation:           kafka.ACLOperationTypeRead,
		PermissionType:      kafka.ACLPermissionTypeAllow,
	}

	scoped := base
	scoped.Host = "10.0.0.7"

	assert.NotEqual(t, aclBindingKey(base), aclBindingKey(scoped),
		"host is a dimension the broker evaluates, so it is part of a binding's identity")

	same := base
	assert.Equal(t, aclBindingKey(base), aclBindingKey(same))

	for _, differing := range []func(kafka.ACLEntry) kafka.ACLEntry{
		func(e kafka.ACLEntry) kafka.ACLEntry { e.ResourceName = "blnk.balances"; return e },
		func(e kafka.ACLEntry) kafka.ACLEntry { e.ResourceType = kafka.ResourceTypeGroup; return e },
		func(e kafka.ACLEntry) kafka.ACLEntry { e.ResourcePatternType = kafka.PatternTypePrefixed; return e },
		func(e kafka.ACLEntry) kafka.ACLEntry { e.Principal = "User:someone-else"; return e },
		func(e kafka.ACLEntry) kafka.ACLEntry { e.Operation = kafka.ACLOperationTypeDescribe; return e },
		func(e kafka.ACLEntry) kafka.ACLEntry { e.PermissionType = kafka.ACLPermissionTypeDeny; return e },
	} {
		assert.NotEqual(t, aclBindingKey(base), aclBindingKey(differing(base)),
			"every dimension the broker evaluates must take part in identity")
	}
}

// TestDescribeSubscriberACLs_ReadsTheCompleteGrantForOnePrincipal covers the read.
//
// A reconciliation that could only see the bindings it EXPECTED could never discover the ones
// it has to remove, so the filter is wide on every dimension except the principal. And the
// principal filter must be genuinely applied: a read that returned another subscriber's
// bindings would turn one reconciliation into somebody else's revocation.
func TestDescribeSubscriberACLs_ReadsTheCompleteGrantForOnePrincipal(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	mine := testSubscriberPrincipal(t)
	theirs := kafkaPrincipalPrefix + "blnk-sub-other"

	fake := newFakeAdminClient().
		withBinding(topicBinding(t, "blnk.transactions", kafka.ACLOperationTypeRead)).
		withBinding(kafka.ACLEntry{
			ResourceType:        kafka.ResourceTypeCluster,
			ResourceName:        "kafka-cluster",
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           mine,
			Host:                "*",
			Operation:           kafka.ACLOperationTypeAlter,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		}).
		withBinding(kafka.ACLEntry{
			ResourceType:        kafka.ResourceTypeTopic,
			ResourceName:        "blnk.identities",
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           theirs,
			Host:                "*",
			Operation:           kafka.ACLOperationTypeRead,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		})

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	bare, err := model.CanonicalKafkaPrincipal(testSubscriberID)
	require.NoError(t, err)

	observed, err := admin.describeSubscriberACLs(context.Background(), bare)
	require.NoError(t, err)

	require.Len(t, observed, 2,
		"the read must return the principal's COMPLETE grant, including the resource types Blnk "+
			"does not provision")
	for _, binding := range observed {
		assert.Equal(t, mine, binding.Principal,
			"another principal's binding must never be returned")
	}

	fake.mu.Lock()
	filter := fake.describeACLsRequests[0].Filter
	fake.mu.Unlock()

	assert.Equal(t, kafka.ResourceTypeAny, filter.ResourceTypeFilter)
	assert.Equal(t, kafka.PatternTypeAny, filter.ResourcePatternTypeFilter)
	assert.Equal(t, kafka.ACLOperationTypeAny, filter.Operation)
	assert.Equal(t, kafka.ACLPermissionTypeAny, filter.PermissionType)
	assert.Equal(t, mine, filter.PrincipalFilter,
		"the principal is the ONE dimension the filter narrows")
	assert.Empty(t, filter.ResourceNameFilter, "an empty name filter is encoded as null and matches anything")
	assert.Empty(t, filter.HostFilter)
}

// TestDescribeSubscriberACLs_TreatsSecurityDisabledAsAFailure keeps AUTH-02 fail-closed.
//
// A broker with no authorizer answers SECURITY_DISABLED to DescribeACLs. Reporting that as "no
// bindings" would let a caller conclude there was nothing to reconcile on precisely the broker
// where nothing is enforced at all — and PruneSubscriberAccess would then report a converged
// grant while every principal could read everything.
func TestDescribeSubscriberACLs_TreatsSecurityDisabledAsAFailure(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	fake.securityDisabled = true
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	bare, err := model.CanonicalKafkaPrincipal(testSubscriberID)
	require.NoError(t, err)

	observed, err := admin.describeSubscriberACLs(context.Background(), bare)
	require.Error(t, err, "an unenforcing broker must not be reported as an empty grant")
	assert.Empty(t, observed)
	assert.ErrorIs(t, err, kafka.SecurityDisabled)

	_, pruneErr := admin.PruneSubscriberAccess(context.Background(), testSubscriber())
	require.Error(t, pruneErr,
		"pruning must fail rather than report a converged grant it could not read")
}

// TestPruneAndGrantSubscriberAccess_AreTheTwoHalvesOfTheSafeOrder covers the three-step
// authorization update.
//
// There is no transaction spanning Blnk and Kafka, so an authorization change is applied as
// prune -> persist -> grant. That order is the only one where every partial failure leaves the
// subscriber with FEWER rights than the registry records: pruning first puts a narrowing in
// force before the row claims it, and granting last lets a widening reach the broker only
// after the row records it. This asserts each half does its own job and NOT the other's.
func TestPruneAndGrantSubscriberAccess_AreTheTwoHalvesOfTheSafeOrder(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	t.Run("prune removes and creates nothing", func(t *testing.T) {
		fake := newFakeAdminClient()
		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		wide := testSubscriber()
		wide.AuthorizedTopics = []string{"blnk.transactions", "blnk.balances", "blnk.identities"}
		_, err := admin.ProvisionSubscriberPrincipal(
			context.Background(),
			NewSubscriberProvisioningRequest(wide, sentinelPassword),
		)
		require.NoError(t, err)

		created := fake.callCount("CreateACLs")

		narrow := testSubscriber()
		report, err := admin.PruneSubscriberAccess(context.Background(), narrow)
		require.NoError(t, err)

		assert.Equal(t, 2, report.Removed)
		assert.Zero(t, report.Created, "prune must never create; that is the third step's job")
		assert.Equal(t, created, fake.callCount("CreateACLs"),
			"a prune that created a binding would put a WIDENING in force before the row records it")
	})

	t.Run("grant creates and removes nothing", func(t *testing.T) {
		fake := newFakeAdminClient()
		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		narrow := testSubscriber()
		narrow.AuthorizedTopics = []string{"blnk.transactions"}
		_, err := admin.ProvisionSubscriberPrincipal(
			context.Background(),
			NewSubscriberProvisioningRequest(narrow, sentinelPassword),
		)
		require.NoError(t, err)

		deletes := fake.callCount("DeleteACLs")

		wide := testSubscriber()
		wide.AuthorizedTopics = []string{"blnk.transactions", "blnk.balances"}
		report, err := admin.GrantSubscriberAccess(context.Background(), wide)
		require.NoError(t, err)

		assert.Equal(t, 2, report.Created, "the newly authorised topic's two bindings")
		assert.Zero(t, report.Removed, "grant must never remove; that is the first step's job")
		assert.Equal(t, deletes, fake.callCount("DeleteACLs"))

		desired, _, err := subscriberDesiredBindings(wide)
		require.NoError(t, err)
		assert.Equal(t, aclKeys(desired), aclKeys(fake.heldBindings()))
	})

	t.Run("grant is idempotent", func(t *testing.T) {
		fake := newFakeAdminClient()
		admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

		subscriber := testSubscriber()
		_, err := admin.ProvisionSubscriberPrincipal(
			context.Background(),
			NewSubscriberProvisioningRequest(subscriber, sentinelPassword),
		)
		require.NoError(t, err)

		report, err := admin.GrantSubscriberAccess(context.Background(), subscriber)
		require.NoError(t, err)

		assert.Zero(t, report.Created,
			"re-granting an unchanged authorization must be a no-op, so a retry after a partial "+
				"failure is safe")
		assert.Equal(t, 5, report.Managed)
	})
}

// TestPruneSubscriberAccess_RefusesARowItCannotDeriveABoundaryFrom keeps derivation single.
//
// The desired set is produced by NewSubscriberProvisioningRequest — the same code provisioning
// binds — so there is exactly ONE definition of the access boundary. A row that cannot yield
// one is refused rather than reconciled against a guess, because a guessed desired set would
// delete the bindings the row actually implies.
func TestPruneSubscriberAccess_RefusesARowItCannotDeriveABoundaryFrom(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	_, nilErr := admin.PruneSubscriberAccess(context.Background(), nil)
	require.Error(t, nilErr)

	unkeyed := &model.EventSubscriber{}
	_, unkeyedErr := admin.PruneSubscriberAccess(context.Background(), unkeyed)
	require.Error(t, unkeyedErr,
		"a row with neither a derivable nor a recorded principal has no boundary to converge on")

	ungrantable := testSubscriber()
	ungrantable.AuthorizedTopics = []string{"blnk.transactions.dlt"}
	_, dltErr := admin.PruneSubscriberAccess(context.Background(), ungrantable)
	require.Error(t, dltErr,
		"a dead-letter topic is not grantable, and reconciliation must refuse the row rather "+
			"than converge on a set it would refuse to create")

	assert.Zero(t, fake.callCount("DeleteACLs"),
		"nothing may be deleted on the strength of a boundary that could not be derived")
}

// TestReconcileAgainstOutbox_RefusesAGreenVerdictWhileAnyRowCannotNameItsRecord is the
// masking guard, and it is the heart of the finding's resolution.
//
// # The defect
//
// The reconciliation compared two totals: records written against rows claiming a publication.
// Records are a lower bound on events, so a surplus is expected — and that is precisely the
// weakness. A SURPLUS OF REDELIVERIES IS ARITHMETICALLY INDISTINGUISHABLE FROM A SURPLUS THAT
// IS MASKING AN EQUAL NUMBER OF LOSSES. Ten redeliveries plus ten lost events produce exactly
// the numbers of a healthy pipeline, so the verdict read "no loss detected" while ten ledger
// events were genuinely gone and nothing in the figures hinted at it.
//
// # The fix, stated as a property
//
// Each row names the record it produced and that coordinate is checked against the measured
// window of its own partition, so the check is a bounded mapping rather than a subtraction:
// while ANY row cannot be placed, the verdict is inconclusive — because those rows are exactly
// what a surplus could be hiding. This test drives the masking scenario directly and requires
// the verdict to refuse it.
//
// It states the two sides rather than measuring them, which is what lets the masking scenario be
// constructed exactly — a thousand rows against a thousand records with ten unplaceable — and a
// live cluster cannot be posed that way. The same refusal over genuinely measured inputs is
// proven by the second arm of
// TestZeroLoss_TheStatisticsProjectionIsAssembledFromLivePostgresAndLiveKafka, where a real
// dispatched row carrying no broker coordinate withdraws a real verdict's confidence.
func TestReconcileAgainstOutbox_RefusesAGreenVerdictWhileAnyRowCannotNameItsRecord(t *testing.T) {
	t.Run("the masking scenario is no longer reported as no loss", func(t *testing.T) {
		// 1,000 rows claim a publication. The broker holds 1,000 records. Under the old
		// arithmetic this is a perfect, green reconciliation.
		//
		// But only 990 of those rows can name a record. The other ten claim a publication
		// nothing corroborates — and the ten records that make the totals balance could just
		// as easily be redeliveries of events that WERE published. The counting cannot tell,
		// so it must not pretend to.
		report := measuredReport("blnk.transactions", 0, 0, 1_000)
		audit := model.EventRecordIntervalAudit{
			PublishedRows:               1_000,
			CorroboratedRows:            990,
			DistinctCorroboratedRecords: 990,
			UnconfirmedRows:             10,
		}

		verdict := ReconcileAgainstOutbox(report, audit)

		assert.False(t, verdict.LossDetected,
			"no record is provably gone, so no loss is DETECTED — the point is that none can be "+
				"ruled out either")
		assert.False(t, verdict.Conclusive,
			"A GREEN VERDICT HERE IS THE DEFECT: ten rows claim a publication nothing corroborates, "+
				"and the ten surplus records could be redeliveries rather than those ten events")
		assert.Equal(t, int64(10), verdict.UnconfirmedEvents)
		require.NotEmpty(t, verdict.Caveats)
		assert.Contains(t, verdict.Summary(), "INCONCLUSIVE")
		assert.Contains(t, strings.Join(verdict.Caveats, " "), "without naming the broker record",
			"the caveat must say what is wrong, so an operator knows what to fix")
	})

	t.Run("a fully corroborated outbox is conclusive", func(t *testing.T) {
		// The same totals, with every claim placed inside a measured window. Now the surplus
		// provably IS overhead, because each of the 1,000 claims names a distinct record of
		// its own inside the window its partition can serve.
		report := measuredReport("blnk.transactions", 0, 0, 1_000)

		verdict := ReconcileAgainstOutbox(report, corroboratedAudit(1_000, auditBoundedByRetainedInterval))

		assert.True(t, verdict.Conclusive)
		assert.Zero(t, verdict.UnconfirmedEvents)
		assert.Equal(t, int64(1_000), verdict.CorroboratedEvents)
		assert.Contains(t, verdict.Summary(), "NO LOSS DETECTED")
	})

	t.Run("the covered window is reported, so a green verdict states its own scope", func(t *testing.T) {
		// A verdict without its window is how a reconciliation over an aggressively pruned
		// outbox came to look complete: the rows it could not see had been deleted, so it
		// reported success over whatever was left without saying so.
		audit := corroboratedAudit(1_000, auditBoundedByRetainedInterval)

		verdict := ReconcileAgainstOutbox(measuredReport("blnk.transactions", 0, 0, 1_000), audit)

		assert.Equal(t, audit.CorroboratedFrom, verdict.CoveredFrom)
		assert.Equal(t, audit.CorroboratedTo, verdict.CoveredTo)
		assert.Equal(t, audit.OldestTerminalAt, verdict.OldestTerminalAt,
			"the oldest retained row bounds what ANY verdict can speak about, so it travels with it")
		assert.Contains(t, verdict.Summary(), audit.CorroboratedFrom.Format(time.RFC3339),
			"the sentence an operator quotes must name the window it covers")
	})

	t.Run("two rows naming one record is reported as a schema fault", func(t *testing.T) {
		// One record is produced by one acknowledged write of one row, and the partial unique
		// index forbids two rows naming the same coordinate — so this state is impossible while
		// that index exists. It is checked rather than assumed because two rows sharing one
		// record's corroboration is the same double-counting the mapping removes, and an
		// absent index would reintroduce it silently.
		report := measuredReport("blnk.transactions", 0, 0, 1_000)
		audit := model.EventRecordIntervalAudit{
			PublishedRows:               1_000,
			CorroboratedRows:            1_000,
			DistinctCorroboratedRecords: 995,
		}

		verdict := ReconcileAgainstOutbox(report, audit)

		assert.Equal(t, int64(5), verdict.DuplicatedRecords)
		assert.False(t, verdict.Conclusive)
		assert.Contains(t, strings.Join(verdict.Caveats, " "), "unique index",
			"the caveat must point at the index whose absence allows this")
	})

	t.Run("loss still takes precedence over inconclusiveness", func(t *testing.T) {
		// A record beyond the log end is unambiguous whatever else is wrong, so it must win
		// the summary: an operator reading "INCONCLUSIVE" would go looking for a measurement
		// problem instead of a recreated topic.
		report := measuredReport("blnk.transactions", 0, 0, 900)
		audit := model.EventRecordIntervalAudit{
			PublishedRows:               1_000,
			CorroboratedRows:            500,
			DistinctCorroboratedRecords: 500,
			UnconfirmedRows:             400,
			BeyondEndRows:               100,
		}

		verdict := ReconcileAgainstOutbox(report, audit)

		assert.True(t, verdict.LossDetected)
		assert.False(t, verdict.Conclusive)
		assert.Contains(t, verdict.Summary(), "LOSS DETECTED")
		assert.Len(t, verdict.Caveats, 2,
			"both reasons must be listed even though only one sets the verdict")
	})

	t.Run("every reason is carried on its own field", func(t *testing.T) {
		// Collapsing any two of these into one number would destroy the distinction an
		// operator acts on: retention is routine, an unmeasured partition is a broker
		// problem, an unconfirmed row is a relay problem, and a beyond-end offset is a
		// recreated topic.
		audit := model.EventRecordIntervalAudit{
			PublishedRows:               100,
			CorroboratedRows:            60,
			DistinctCorroboratedRecords: 60,
			UnconfirmedRows:             10,
			UnmeasuredRows:              12,
			AgedOutRows:                 15,
			BeyondEndRows:               3,
		}

		verdict := ReconcileAgainstOutbox(measuredReport("blnk.transactions", 0, 0, 100), audit)

		assert.Equal(t, int64(10), verdict.UnconfirmedEvents)
		assert.Equal(t, int64(12), verdict.UnmeasuredEvents)
		assert.Equal(t, int64(15), verdict.AgedOutEvents)
		assert.Equal(t, int64(3), verdict.BeyondEndEvents)
		assert.Equal(t, int64(60), verdict.CorroboratedEvents)
		assert.Equal(t, verdict.TerminalEvents,
			verdict.CorroboratedEvents+verdict.UnconfirmedEvents+verdict.UnmeasuredEvents+
				verdict.AgedOutEvents+verdict.BeyondEndEvents,
			"the buckets must partition the claims, or the verdict is describing a set that is "+
				"not the outbox")
		assert.Len(t, verdict.Caveats, 4, "one caveat per reason, each in its own words")
	})
}

// TestEventRecordIntervalAudit_DerivesItsOwnConclusions pins the two derived answers, because
// both are read as guards and an off-by-one in either would change a verdict.
func TestEventRecordIntervalAudit_DerivesItsOwnConclusions(t *testing.T) {
	t.Run("uncorroborated rows sum every reason", func(t *testing.T) {
		audit := model.EventRecordIntervalAudit{
			PublishedRows:    10,
			CorroboratedRows: 4,
			UnconfirmedRows:  1,
			UnmeasuredRows:   2,
			AgedOutRows:      2,
			BeyondEndRows:    1,
		}
		assert.Equal(t, int64(6), audit.UncorroboratedRows())
	})

	t.Run("duplicated records never go negative", func(t *testing.T) {
		// DistinctCorroboratedRecords can only exceed CorroboratedRows if the counts were read
		// from different queries, and a negative "duplicated" would be reported as a caveat
		// describing duplication that did not occur.
		audit := model.EventRecordIntervalAudit{
			CorroboratedRows: 10, DistinctCorroboratedRecords: 12,
		}
		assert.Zero(t, audit.DuplicatedRecords())
	})

	t.Run("fully corroborated requires both properties", func(t *testing.T) {
		assert.True(t, model.EventRecordIntervalAudit{
			PublishedRows: 5, CorroboratedRows: 5, DistinctCorroboratedRecords: 5,
		}.FullyCorroborated())

		assert.False(t, model.EventRecordIntervalAudit{
			PublishedRows: 5, CorroboratedRows: 4, DistinctCorroboratedRecords: 4, UnconfirmedRows: 1,
		}.FullyCorroborated(), "an unplaced claim is enough to disqualify it")

		assert.False(t, model.EventRecordIntervalAudit{
			PublishedRows: 5, CorroboratedRows: 5, DistinctCorroboratedRecords: 4,
		}.FullyCorroborated(), "so is a shared coordinate")
	})

	t.Run("nothing published is fully corroborated", func(t *testing.T) {
		assert.True(t, model.EventRecordIntervalAudit{}.FullyCorroborated())
	})
}

// TestBrokerRecord_DistinguishesAbsenceFromTheFirstRecordOnAPartition pins the one distinction
// the whole coordinate mechanism rests on.
//
// Partition 0, offset 0 is a REAL and very ordinary location — the first record on a fresh
// partition — so a zero-value test on the numbers alone would report a genuine coordinate as
// absent, and the row that produced the first record on every partition would be counted as an
// unconfirmed publication forever.
func TestBrokerRecord_DistinguishesAbsenceFromTheFirstRecordOnAPartition(t *testing.T) {
	first := model.BrokerRecord{Topic: "blnk.transactions", Partition: 0, Offset: 0}
	assert.True(t, first.Confirmed(),
		"the first record on partition 0 is a real location and must not read as absent")
	assert.Equal(t, "blnk.transactions/0@0", first.String())

	assert.False(t, model.BrokerRecord{}.Confirmed())
	assert.Equal(t, "unconfirmed", model.BrokerRecord{}.String(),
		"an absent coordinate must render as a named absence, not as /0@0 which reads as data")

	assert.False(t, model.BrokerRecord{Partition: 3, Offset: 91}.Confirmed(),
		"a coordinate with no topic cannot be looked up, so it is not confirmed")
	assert.False(t, model.BrokerRecord{Topic: "   ", Offset: 1}.Confirmed(),
		"a blank topic is no topic")
	assert.False(t, model.BrokerRecord{Topic: "blnk.transactions", Offset: -1}.Confirmed(),
		"-1 is kafka-go's own 'no offset' sentinel and must not be stored as a location")
}

// TestEventOutbox_BrokerRecordIsAllOrNothing pins the row accessor.
//
// A half-written coordinate is worse than none: a reader testing only the offset would treat a
// row with no topic as confirmed and then have nothing to look the record up with, so the
// audit's confirmed count would include rows it cannot actually match. The schema enforces this
// with a check constraint; the accessor states the same rule in one place so no reader has to
// test three pointers itself.
func TestEventOutbox_BrokerRecordIsAllOrNothing(t *testing.T) {
	partition := 4
	offset := int64(9_182)

	complete := model.EventOutbox{
		KafkaTopic:     "blnk.balances",
		KafkaPartition: &partition,
		KafkaOffset:    &offset,
	}
	record, ok := complete.BrokerRecord()
	require.True(t, ok)
	assert.Equal(t, "blnk.balances/4@9182", record.String())

	for name, row := range map[string]model.EventOutbox{
		"nothing recorded": {},
		"topic only":       {KafkaTopic: "blnk.balances"},
		"no topic":         {KafkaPartition: &partition, KafkaOffset: &offset},
		"no offset":        {KafkaTopic: "blnk.balances", KafkaPartition: &partition},
		"no partition":     {KafkaTopic: "blnk.balances", KafkaOffset: &offset},
		"blank topic":      {KafkaTopic: "  ", KafkaPartition: &partition, KafkaOffset: &offset},
	} {
		t.Run("incomplete: "+name, func(t *testing.T) {
			_, present := row.BrokerRecord()
			assert.False(t, present,
				"a partial coordinate must not read as confirmed; there would be nothing to look up")
		})
	}
}

// ---------------------------------------------------------------------------------------
// Least privilege — PRIV-01
// ---------------------------------------------------------------------------------------

// TestKafkaTransportCredentials_RefusesToPublishAsTheAdministrator is the PRIV-01 guard.
//
// # The defect
//
// The steady-state publisher fell back to KAFKA_SASL_ADMIN_USER when no producer pair was
// configured — the principal that creates topics, alters SCRAM credentials and manages ACLs.
// Every ledger event was therefore produced by the most privileged identity in the system, so a
// leaked producer credential handed an attacker the ability to rewrite the access model rather
// than merely to publish events, and the broker's audit trail could not distinguish routine
// publishing from administration. The local Compose stack made that fallback the default path.
//
// It warned every time, and that was still the wrong answer: a warning is advice, the code went
// on publishing as the superuser regardless, and the posture therefore held only for operators
// who read a log line and acted on it.
func TestKafkaTransportCredentials_RefusesToPublishAsTheAdministrator(t *testing.T) {
	t.Run("an admin pair with no producer pair is refused", func(t *testing.T) {
		cfg := config.KafkaConfig{
			Brokers:         []string{"broker:9092"},
			SASLAdminUser:   "blnk-kafka-admin",
			SASLAdminSecret: "an-admin-secret",
		}

		user, secret, err := kafkaTransportCredentials(cfg, KafkaTransportRoleProducer)
		require.Error(t, err,
			"THE FALLBACK IS THE DEFECT: publishing as the administrator must fail closed, not warn")
		assert.Empty(t, user, "no credential may be resolved from a refusal")
		assert.Empty(t, secret)

		assert.Contains(t, err.Error(), "KAFKA_SASL_USER",
			"the error must name the variable to set")
		assert.Contains(t, err.Error(), "blnk-kafka-admin",
			"and the principal it refused to use, which is an identifier rather than a credential")
		assert.NotContains(t, err.Error(), "an-admin-secret",
			"the secret must never appear in an error a caller will log")
	})

	t.Run("a producer pair is used and the admin pair is not consulted", func(t *testing.T) {
		cfg := config.KafkaConfig{
			Brokers:         []string{"broker:9092"},
			SASLAdminUser:   "blnk-kafka-admin",
			SASLAdminSecret: "an-admin-secret",
			SASLUser:        "blnk-producer",
			SASLSecret:      "a-producer-secret",
		}

		user, secret, err := kafkaTransportCredentials(cfg, KafkaTransportRoleProducer)
		require.NoError(t, err)
		assert.Equal(t, "blnk-producer", user,
			"the steady state publishes as a principal that cannot alter the access model")
		assert.Equal(t, "a-producer-secret", secret)
	})

	t.Run("no SASL anywhere resolves to no credentials", func(t *testing.T) {
		// An unauthenticated local broker. Refusing here would break the documented plaintext
		// local-development configuration, which is a legitimate shape rather than the one
		// PRIV-01 is about.
		user, secret, err := kafkaTransportCredentials(
			config.KafkaConfig{Brokers: []string{"localhost:9092"}},
			KafkaTransportRoleProducer,
		)
		require.NoError(t, err)
		assert.Empty(t, user)
		assert.Empty(t, secret)
	})

	t.Run("the admin role still resolves the admin pair", func(t *testing.T) {
		// Provisioning genuinely needs the administrator, so PRIV-01 must not disarm it.
		cfg := config.KafkaConfig{
			Brokers:         []string{"broker:9092"},
			SASLAdminUser:   "blnk-kafka-admin",
			SASLAdminSecret: "an-admin-secret",
		}

		user, secret, err := kafkaTransportCredentials(cfg, KafkaTransportRoleAdmin)
		require.NoError(t, err)
		assert.Equal(t, "blnk-kafka-admin", user)
		assert.Equal(t, "an-admin-secret", secret)
	})

	t.Run("a half-configured producer pair is refused for its own reason", func(t *testing.T) {
		// A username with no secret is a configuration mistake, and it must be reported as
		// that rather than silently falling through to the admin refusal — which would send an
		// operator to the wrong line of their configuration.
		_, _, err := kafkaTransportCredentials(config.KafkaConfig{
			Brokers:  []string{"broker:9092"},
			SASLUser: "blnk-producer",
		}, KafkaTransportRoleProducer)
		require.Error(t, err)
		assert.NotContains(t, err.Error(), "administrative principal",
			"a half-configured pair is not the admin-fallback refusal")
	})
}

// exportedLagSeries is what the asynchronous consumer-lag gauges will export for one
// measured topic: the resolved label tuple, the value, and whether the value is exported
// at all.
//
// It replaces a fake instrument that captured Record calls. That fake could not survive the
// instrument becoming ASYNCHRONOUS — an observable gauge has no Record, because a measurement
// is read from a published inventory at collection time rather than written when it is taken
// (see metrics.SubscriberConsumerLag). The projection under test is therefore
// ConsumerLagReport.LagSamples, which is the exact value the inventory is built from, and
// asserting it directly is both simpler and stricter than asserting a mock's call log.
type exportedLagSeries struct {
	subscriber string
	group      string
	topic      string

	// lag is the value, meaningful only when lagExported is true.
	lag int64

	// lagExported is false for a topic whose partitions could not all be read. Such a
	// sample contributes measurement health and NO lag, because a partial sum is a lower
	// bound and publishing it would resolve the >10000 alert with a figure known to be too
	// small.
	lagExported bool

	// unmeasuredPartitions is always exported, zero included.
	unmeasuredPartitions int
}

// TestConsumerLagSample_ResolvesEveryLabelAndNeedsNoInstrument replaces a test that pinned a
// nil-instrument guard, because the guard is no longer reachable and the property it protected
// is now structural.
//
// Building a sample is PURE: it touches no instrument, so a build in which the instruments were
// never created cannot panic here at all, and there is nothing left to guard. What has to be
// asserted instead is the label resolution, which is the reason this function exists — it is
// the last gate before three registry-derived values become exported label dimensions, and the
// only gate that covers a value which never came from the registry.
func TestConsumerLagSample_ResolvesEveryLabelAndNeedsNoInstrument(t *testing.T) {
	original := metrics.SubscriberConsumerLag
	t.Cleanup(func() { metrics.SubscriberConsumerLag = original })

	// Nil instruments, to state that sample construction is independent of them.
	metrics.SubscriberConsumerLag = nil

	var sample metrics.ConsumerLagSample
	assert.NotPanics(t, func() {
		sample = consumerLagSample("sub_0f6e2c8a", "blnk-sub-sub_0f6e2c8a.default", TopicLag{
			Topic:      "blnk.transactions",
			TotalLag:   7,
			Partitions: []PartitionLag{{Topic: "blnk.transactions"}},
		})
	})

	// PSEUDONYMOUS, and asserted through the resolvers rather than against a literal digest:
	// the property is that a registry identifier never becomes a label value, not that the
	// hash is computed one particular way. See subscriberLagLabel for why a label is the
	// widest thing this process emits and therefore the wrong place for a tenant's name.
	assert.Equal(t, subscriberLagLabel("sub_0f6e2c8a"), sample.Subscriber)
	assert.NotEqual(t, "sub_0f6e2c8a", sample.Subscriber,
		"the registry identifier must not reach the gauge as a label value")
	assert.Equal(t, consumerGroupLagLabel("blnk-sub-sub_0f6e2c8a.default"), sample.Group,
		"the group collapses to its subscriber-scoped root before it is hashed, so one subscriber is one series")
	assert.NotContains(t, sample.Group, "sub_0f6e2c8a",
		"and the group label must not carry the subscriber identifier by the back door")
	assert.Equal(t, "blnk.transactions", sample.Topic,
		"the topic is Blnk's own and is a closed set, so it is exported as itself")
	assert.Equal(t, int64(7), sample.Lag)
	assert.True(t, sample.LagComplete, "no partition was unavailable, so the lag is exportable")
	assert.Zero(t, sample.UnmeasuredPartitions)

	// An UNRECOGNISED value collapses to a fixed token rather than being exported. All three
	// collapse tokens are named in alerts/blnk-kafka-alerts.yml's annotation so an operator who
	// is paged with one knows what it means, which is why they are asserted as literals here.
	collapsed := consumerLagSample("Acme Payments PROD", "acme-recon-group", TopicLag{Topic: "attacker.topic"})
	assert.Equal(t, "unregistered", collapsed.Subscriber,
		"a subscriber id outside the permitted identifier alphabet must not reach an exported label")
	assert.Equal(t, "unregistered", collapsed.Group,
		"a group outside a namespace Blnk reserves collapses rather than exporting a chosen name")
	assert.Equal(t, "other", collapsed.Topic,
		"a topic Blnk does not own collapses, so a foreign name cannot mint a series")

	// An UNNAMED measurement — an operator's ad-hoc query — is distinguishable from a
	// rejected one, because the two call for different responses: 'unattributed' is expected,
	// while 'unregistered' is itself worth investigating.
	unnamed := consumerLagSample("", "", TopicLag{Topic: "blnk.transactions"})
	assert.Equal(t, "unattributed", unnamed.Subscriber)
	assert.Equal(t, "unattributed", unnamed.Group)
	assert.Equal(t, "blnk.transactions", unnamed.Topic, "an owned topic is still reported verbatim")
}

// TestConsumerLag_WithholdsAPartialTotalFromTheGaugeTheAlertReads is the alerting-integrity
// requirement, and it is the half that marking the partition unavailable does not achieve.
//
// # The failure this prevents
//
// Marking a partition unavailable makes the DEGRADATION legible in the report. It does nothing
// about the number, and the number was still being published: TotalLag is a sum, so an
// unreadable partition contributes zero and the total silently becomes a LOWER BOUND, which
// reached the gauge exactly as though it were a complete measurement.
//
// That is worse than publishing nothing, because the alert is a threshold comparison. The
// arithmetic below is the whole point: a subscriber genuinely 60,000 messages behind across six
// partitions reads as 10,000 when five of them go unreadable — under the >10000 rule — so a
// broker or metadata fault does not merely blind the measurement, it RESOLVES a firing alert
// and reports the system as healthy at the moment it is least able to tell. An operator
// watching the alert would see it clear and conclude the consumer had caught up.
//
// So the sample is marked incomplete and its lag is withheld, and the unmeasured-partition
// count is published in its place. The condition then surfaces as ConsumerLagMeasurementDegraded
// rather than as good news, and the partial total remains in the report and the log for
// diagnosis — withheld from telemetry, not discarded.
func TestConsumerLag_WithholdsAPartialTotalFromTheGaugeTheAlertReads(t *testing.T) {
	// Mirrors alerts/blnk-kafka-alerts.yml. Written out because the rule is PromQL and not Go:
	// asserting the number on both sides is the only way the two stay in agreement.
	const alertThreshold = int64(10_000)

	const (
		endOffset       = int64(100_000)
		perPartitionLag = int64(10_000)
		unreadable      = 5
	)

	fake := newFakeAdminClient()
	failures := map[int]error{}
	for partition := 0; partition < MinTopicPartitions; partition++ {
		fake.withOffsets("blnk.transactions", partition, 0, endOffset)
		fake.withCommitted("blnk.transactions", partition, endOffset-perPartitionLag)

		// Every partition but the first is unreadable.
		if partition > 0 {
			failures[partition] = kafka.LeaderNotAvailable
		}
	}
	fake.offsetErrors["blnk.transactions"] = failures

	// A second topic is measured alongside and is FULLY readable, because the withholding must
	// be per topic. Suppressing a whole subscriber's telemetry because one of its topics is
	// degraded would blind the alert for topics that are being measured perfectly well.
	fake.withOffsets("blnk.balances", 0, 0, 50_000).withCommitted("blnk.balances", 0, 20_000)

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.ConsumerLag(context.Background(), ConsumerLagRequest{
		SubscriberID: "sub_0f6e2c8a",
		GroupID:      "blnk-sub-sub_0f6e2c8a.default",
		Topics:       []string{"blnk.transactions", "blnk.balances"},
	})
	require.NoError(t, err, "an incomplete measurement is degraded, not failed")

	require.Len(t, report.Topics, 2)
	degraded, healthy := report.Topics[0], report.Topics[1]

	// THE FIXTURE'S OWN ARITHMETIC, asserted so the test cannot become vacuous. The true lag is
	// above the threshold and the partial total is not, which is the exact configuration in
	// which publishing the partial total would resolve the alert.
	require.Equal(t, unreadable, degraded.PartitionsUnavailable)
	require.Equal(t, perPartitionLag, degraded.TotalLag,
		"only the one readable partition contributed, so the total is a lower bound")
	require.Greater(t, perPartitionLag*int64(MinTopicPartitions), alertThreshold,
		"the TRUE lag must exceed the threshold, or there is no alert to wrongly resolve")
	require.LessOrEqual(t, degraded.TotalLag, alertThreshold,
		"and the partial total must NOT exceed it, or this test proves nothing about withholding")

	series := exportedLagSeriesFor(report)
	require.Len(t, series, 2, "both topics remain in the inventory; withholding is about the lag, not the series")

	// The degraded topic: present, no lag, and its unmeasured count published.
	assert.False(t, series[0].lagExported,
		"the partial total must NOT reach the gauge the >10000 rule reads")
	assert.Equal(t, unreadable, series[0].unmeasuredPartitions,
		"the unmeasured-partition count is what fires ConsumerLagMeasurementDegraded in its place")
	assert.Equal(t, "blnk.transactions", series[0].topic)

	// The healthy topic, measured in the same call: unaffected.
	assert.True(t, series[1].lagExported, "a fully readable topic must still export its lag")
	assert.Equal(t, int64(30_000), series[1].lag)
	assert.Zero(t, series[1].unmeasuredPartitions)
	assert.Equal(t, healthy.TotalLag, series[1].lag)

	// And the only value the alert can read is the trustworthy one.
	assert.Equal(t, []int64{30_000}, exportedLags(series),
		"exactly one lag reading is exportable, and it is the complete one")
}

// exportedLagSeriesFor renders a report as the series the gauges will export.
//
// Parameters:
//   - report ConsumerLagReport: the measurement to project.
//
// Returns:
//   - []exportedLagSeries: one entry per measured topic, in report order.
func exportedLagSeriesFor(report ConsumerLagReport) []exportedLagSeries {
	samples := report.LagSamples()

	series := make([]exportedLagSeries, 0, len(samples))
	for _, sample := range samples {
		series = append(series, exportedLagSeries{
			subscriber:           sample.Subscriber,
			group:                sample.Group,
			topic:                sample.Topic,
			lag:                  sample.Lag,
			lagExported:          sample.LagComplete,
			unmeasuredPartitions: sample.UnmeasuredPartitions,
		})
	}

	return series
}

// exportedLags reduces a projection to the lag values that are actually exported, dropping
// every withheld one.
//
// It is what an assertion about "what the alert can read" needs: a withheld sample is not a
// zero and must not be counted as one.
//
// Returns:
//   - []int64: the exported lag values, in report order.
func exportedLags(series []exportedLagSeries) []int64 {
	lags := make([]int64, 0, len(series))
	for _, one := range series {
		if one.lagExported {
			lags = append(lags, one.lag)
		}
	}

	return lags
}

// ---------------------------------------------------------------------------
// Event pipeline statistics — the orchestration behind GET /events/stats
// ---------------------------------------------------------------------------

// statsFakeStore is an in-memory eventStatisticsStore.
//
// It records the calls as well as answering them, because half of what the
// orchestration decides is WHETHER a read happens at all: the audit is only wanted
// when the broker side is going to be measured, and a skipped posture must make no
// broker round trip. Neither of those is observable from the returned value.
type statsFakeStore struct {
	counts map[string]int64
	audit  model.EventRecordIntervalAudit

	handoff   map[string]int64
	batches   int64
	batchesAt *time.Time

	countErr   error
	auditErr   error
	handoffErr error
	batchesErr error

	countCalls     int
	auditCalls     int
	countedSince   time.Time
	censusAttempts int

	// historyCalls and unresolvedCalls separate the TWO aggregates, which is the substance of
	// PERF-M05 at this layer: a posture that skips the broker side must take the unresolved
	// aggregate — exact, unwindowed, bounded by outstanding work — and must never reach the
	// one whose second arm counts 43.2 million dispatched index entries a day at the target
	// rate. countCalls stays the total of the two, so every existing assertion on "was the
	// count read at all" keeps its meaning.
	historyCalls    int
	unresolvedCalls int
}

func (s *statsFakeStore) CountEventOutboxByStatus(
	_ context.Context,
	since time.Time,
) (map[string]int64, error) {
	s.countCalls++
	s.historyCalls++
	s.countedSince = since

	if s.countErr != nil {
		return nil, s.countErr
	}

	return s.counts, nil
}

// CountUnresolvedEventOutbox answers the lightweight aggregate, and OMITS the dispatched key.
//
// The omission is the whole reason this is a separate fake method rather than an alias: the real
// query has no dispatched arm, so a fake that returned the same map as the history reading would
// let a projection bug — reporting a dispatched figure that was never counted — pass every test.
// It also records no `since`, because there is none to record.
func (s *statsFakeStore) CountUnresolvedEventOutbox(_ context.Context) (map[string]int64, error) {
	s.countCalls++
	s.unresolvedCalls++

	if s.countErr != nil {
		return nil, s.countErr
	}

	unresolved := make(map[string]int64, len(s.counts))
	for status, count := range s.counts {
		if status == model.EventOutboxStatusDispatched {
			continue
		}
		unresolved[status] = count
	}

	return unresolved, nil
}

func (s *statsFakeStore) CountBalanceMonitorHandoffByStatus(
	context.Context,
) (map[string]int64, error) {
	s.censusAttempts++
	if s.handoffErr != nil {
		return nil, s.handoffErr
	}

	return s.handoff, nil
}

func (s *statsFakeStore) CountUnfinalizedBulkTransactionBatches(
	context.Context,
	time.Duration,
) (int64, *time.Time, error) {
	if s.batchesErr != nil {
		return 0, nil, s.batchesErr
	}

	return s.batches, s.batchesAt, nil
}

func (s *statsFakeStore) AuditEventRecordsInIntervals(
	context.Context,
	[]model.PartitionOffsetInterval,
) (model.EventRecordIntervalAudit, error) {
	s.auditCalls++
	if s.auditErr != nil {
		return model.EventRecordIntervalAudit{}, s.auditErr
	}

	return s.audit, nil
}

// statsOffsetReader builds an offset reader that records how often it was called.
//
// It records the WINDOW it was asked for as well, because that argument is what makes the
// broker side and the outbox side describe one population: a cumulative broker reading
// compared against a windowed outbox count is a shortfall manufactured by the question.
func statsOffsetReader(
	report TopicOffsetReport,
	err error,
	calls *int,
) func(context.Context, time.Time) (TopicOffsetReport, error) {
	return func(_ context.Context, _ time.Time) (TopicOffsetReport, error) {
		*calls++

		return report, err
	}
}

// statsWindowRecordingOffsetReader is statsOffsetReader with the window captured.
func statsWindowRecordingOffsetReader(
	report TopicOffsetReport,
	since *time.Time,
) func(context.Context, time.Time) (TopicOffsetReport, error) {
	return func(_ context.Context, windowStart time.Time) (TopicOffsetReport, error) {
		*since = windowStart

		return report, nil
	}
}

// statsMeasuredReport is a broker measurement covering one topic, which is the minimum
// that makes a verdict producible.
func statsMeasuredReport() TopicOffsetReport {
	return TopicOffsetReport{
		Topics: []TopicOffsetSnapshot{{
			Topic:         "blnk.transactions",
			EndOffsetSum:  12,
			RetainedCount: 12,
		}},
		EndOffsetSum:  12,
		RetainedCount: 12,
		MeasuredAt:    time.Now().UTC(),
	}
}

// TestEventOutboxStatistics_ReadsTheOutboxFirstAndTheBrokerAsAnEnrichment pins the
// sequencing the whole read depends on.
//
// The counts come from PostgreSQL and their failure is the only unconditional one:
// without them there is nothing to report. Everything after them is an enrichment,
// which is what allows a deployment with NO BROKERS — a legitimate steady state, not a
// fault — to answer with the counts alone.
//
// The audit is read only when the broker side is going to be measured, because its
// only consumer is the comparison against the offsets. That is asserted by call count
// rather than by return value, since a wasted query is invisible in the result.
func TestEventOutboxStatistics_ReadsTheOutboxFirstAndTheBrokerAsAnEnrichment(t *testing.T) {
	t.Run("a skipped posture reads neither the audit nor the broker", func(t *testing.T) {
		store := &statsFakeStore{counts: map[string]int64{
			model.EventOutboxStatusPending:      3,
			model.EventOutboxStatusDispatched:   9,
			model.EventOutboxStatusDeadLettered: 1,
		}}
		offsetCalls := 0

		statistics, err := eventOutboxStatistics(context.Background(), store,
			statsOffsetReader(statsMeasuredReport(), nil, &offsetCalls), EventOffsetsSkipped, 0)
		require.NoError(t, err)

		assert.Equal(t, 1, store.countCalls)
		assert.Zero(t, store.auditCalls,
			"the audit's only consumer is the comparison against the offsets, so a skipped posture must not pay for it")
		assert.Zero(t, offsetCalls, "a skipped posture must make no broker round trip at all")

		assert.False(t, statistics.AuditRead)
		assert.False(t, statistics.OffsetsRead)
		assert.Nil(t, statistics.Reconciliation)
		assert.Equal(t, int64(3), statistics.CountsByStatus[model.EventOutboxStatusPending])
		assert.False(t, statistics.GeneratedAt.IsZero(), "the outbox side is always stamped")

		// AND IT MUST NOT PAY FOR THE HISTORY EITHER (PERF-M05). The unresolved inventory is
		// bounded by outstanding work and is index-only; the dispatched arm is an exact count
		// over the one unbounded population — 43.2 million index entries a day at the target
		// rate — and a posture that reads no broker has nothing to compare it against.
		assert.Equal(t, 1, store.unresolvedCalls,
			"a skipped posture must take the UNRESOLVED aggregate")
		assert.Zero(t, store.historyCalls,
			"and must never reach the aggregate whose second arm counts dispatched history")
		assert.False(t, statistics.DispatchedHistoryCounted,
			"and must say so, because an absent dispatched key otherwise reads as 'none were "+
				"dispatched' — which is the opposite of the truth and would report total loss")
		assert.NotContains(t, statistics.CountsByStatus, model.EventOutboxStatusDispatched,
			"the key must be absent rather than zero, matching the query that produced it")
	})

	// The inverse, so the coupling is pinned in both directions: the two postures that measure
	// the broker MUST count the history, because the dispatched figure exists only to be
	// compared against what the broker recorded (PERF-M05).
	for name, inclusion := range map[string]EventOffsetInclusion{
		"a best-effort posture": EventOffsetsBestEffort,
		"a required posture":    EventOffsetsRequired,
	} {
		t.Run(name+" counts the dispatched history", func(t *testing.T) {
			store := &statsFakeStore{counts: map[string]int64{
				model.EventOutboxStatusPending:    3,
				model.EventOutboxStatusDispatched: 9,
			}}
			offsetCalls := 0

			statistics, err := eventOutboxStatistics(context.Background(), store,
				statsOffsetReader(statsMeasuredReport(), nil, &offsetCalls), inclusion, 0)
			require.NoError(t, err)

			assert.Equal(t, 1, store.historyCalls,
				"a posture that measures the broker must count the outbox side it is compared against")
			assert.Zero(t, store.unresolvedCalls,
				"and must take ONE aggregate rather than both: the history reading already "+
					"contains every unresolved count")
			assert.True(t, statistics.DispatchedHistoryCounted)
			assert.Equal(t, int64(9), statistics.CountsByStatus[model.EventOutboxStatusDispatched])
			assert.Equal(t, int64(3), statistics.CountsByStatus[model.EventOutboxStatusPending],
				"and the unresolved counts are still exact and complete in that reading")
		})
	}

	// The ZERO VALUE is the cheap posture, which is what keeps the Go default and the HTTP
	// default from documenting different behaviour (PERF-M02). It used to be best-effort, so a
	// caller that constructed the type without thinking made a broker round trip and a
	// history-sized count.
	t.Run("the zero value of the posture is skipped", func(t *testing.T) {
		var posture EventOffsetInclusion

		assert.Equal(t, EventOffsetsSkipped, posture,
			"the cheapest, most failure-free posture must be what a caller gets by saying nothing")
		assert.False(t, posture.countsDispatchedHistory(),
			"and it must not count the one unbounded population")
		assert.True(t, EventOffsetsBestEffort.countsDispatchedHistory())
		assert.True(t, EventOffsetsRequired.countsDispatchedHistory())
	})

	t.Run("a best-effort posture degrades when the broker cannot be read", func(t *testing.T) {
		store := &statsFakeStore{counts: map[string]int64{model.EventOutboxStatusDispatched: 4}}
		offsetCalls := 0

		statistics, err := eventOutboxStatistics(context.Background(), store,
			statsOffsetReader(TopicOffsetReport{}, ErrKafkaAdminNotConfigured, &offsetCalls),
			EventOffsetsBestEffort, 0)
		require.NoError(t, err,
			"no broker is a legitimate steady state, so it must not turn a statistics read into a failure")

		assert.Equal(t, 1, offsetCalls)
		assert.False(t, statistics.OffsetsRead,
			"an unmeasured broker side must be reported as unmeasured, not as a measured zero")
		assert.Nil(t, statistics.Reconciliation,
			"there is nothing to compare against, so no verdict may be reported")

		// AND THE AUDIT IS UNREAD TOO, which is a consequence of a later fix rather than of this
		// one. The audit is taken against the partition INTERVALS the offset report measured —
		// AuditEventRecordsInIntervals — because an audit over the whole table and an offset
		// total describe different populations and cannot be compared, which is the defect the
		// interval form closes. With no report there are no intervals, so there is nothing to
		// audit and reporting it as read would assert a measurement nobody took.
		assert.False(t, statistics.AuditRead,
			"the audit is scoped to the intervals the offset report measured, so an unread broker "+
				"leaves nothing to audit")
		assert.Zero(t, store.auditCalls,
			"and it must not be attempted over a population the offsets cannot bound")
	})

	t.Run("a required posture refuses when the broker cannot be read", func(t *testing.T) {
		store := &statsFakeStore{counts: map[string]int64{model.EventOutboxStatusDispatched: 4}}
		offsetCalls := 0

		_, err := eventOutboxStatistics(context.Background(), store,
			statsOffsetReader(TopicOffsetReport{}, errors.New("dial tcp 10.0.0.4:9092: connect: refused"), &offsetCalls),
			EventOffsetsRequired, 0)

		dltAssertCodeAndStatus(t, err, apierror.ErrKafkaUnavailable, http.StatusServiceUnavailable)

		// DATA-01: the broker's own words name addresses and topology, so they must stay
		// in the log rather than travel in the error a handler renders.
		assert.NotContains(t, err.Error(), "10.0.0.4",
			"a Kafka client error must not carry broker addresses into the response")
	})

	t.Run("a required posture refuses when the audit cannot be read", func(t *testing.T) {
		store := &statsFakeStore{
			counts:   map[string]int64{model.EventOutboxStatusDispatched: 4},
			auditErr: apierror.NewAPIError(apierror.ErrInternalServer, "audit failed", errors.New("boom")),
		}
		offsetCalls := 0

		_, err := eventOutboxStatistics(context.Background(), store,
			statsOffsetReader(statsMeasuredReport(), nil, &offsetCalls), EventOffsetsRequired, 0)

		dltAssertCodeAndStatus(t, err, apierror.ErrInternalServer, http.StatusInternalServerError)

		// THE BROKER IS READ FIRST, and that is the interval form's doing rather than an
		// oversight. The audit is scoped to the partition intervals the offset report measured,
		// so the report has to exist before the audit can be asked for anything comparable. What
		// the posture decides is whether a failed audit is FATAL — and it is, here, because a
		// required verdict with no outbox side would be a clean bill of health nobody
		// established.
		assert.Equal(t, 1, offsetCalls,
			"the audit is bounded by the intervals the offsets measured, so the broker read "+
				"necessarily precedes it")
	})

	t.Run("a failure to read the counts is unconditional", func(t *testing.T) {
		for _, inclusion := range []EventOffsetInclusion{
			EventOffsetsBestEffort, EventOffsetsRequired, EventOffsetsSkipped,
		} {
			store := &statsFakeStore{countErr: apierror.NewAPIError(
				apierror.ErrInternalServer, "Failed to count", errors.New("dial tcp: refused"),
			)}
			offsetCalls := 0

			_, err := eventOutboxStatistics(context.Background(), store,
				statsOffsetReader(statsMeasuredReport(), nil, &offsetCalls), inclusion, 0)

			dltAssertCodeAndStatus(t, err, apierror.ErrInternalServer, http.StatusInternalServerError)
			assert.Zero(t, store.auditCalls)
			assert.Zero(t, offsetCalls)
		}
	})

	t.Run("a nil store is reported rather than dereferenced", func(t *testing.T) {
		_, err := eventOutboxStatistics(context.Background(), nil, nil, EventOffsetsBestEffort, 0)
		dltAssertCodeAndStatus(t, err, apierror.ErrInternalServer, http.StatusInternalServerError)
	})

	t.Run("a nil offset reader degrades exactly like an unconfigured broker", func(t *testing.T) {
		store := &statsFakeStore{counts: map[string]int64{model.EventOutboxStatusDispatched: 1}}

		statistics, err := eventOutboxStatistics(
			context.Background(), store, nil, EventOffsetsBestEffort, 0,
		)
		require.NoError(t, err)
		assert.False(t, statistics.OffsetsRead)
	})
}

// TestEventOutboxStatistics_ProducesAVerdictOnlyWhenBothSidesWereMeasured is the
// honesty requirement the zero-loss criterion rests on.
//
// A verdict computed over zero topics would report a clean bill of health it never
// established. "Measured and zero" and "not measured" are different answers, and the
// numbers alone cannot distinguish them — which is why the two booleans exist and why
// the verdict is absent rather than green when nothing was covered.
//
// The absence cases are the ones a live stack cannot produce on demand: a broker that answers
// about no topics, an audit that was never taken, a posture that skipped Kafka entirely. The
// PRESENCE case — both sides measured, a verdict produced, and its figures drawn from the same
// rows the census counted — is proven against a live broker and a live database by
// TestZeroLoss_TheStatisticsProjectionIsAssembledFromLivePostgresAndLiveKafka, which calls this
// very orchestration with the real repository and the real offset reader.
func TestEventOutboxStatistics_ProducesAVerdictOnlyWhenBothSidesWereMeasured(t *testing.T) {
	audit := model.EventRecordIntervalAudit{
		PublishedRows:               10,
		CorroboratedRows:            10,
		DistinctCorroboratedRecords: 10,
	}

	t.Run("both sides measured yields the verdict", func(t *testing.T) {
		store := &statsFakeStore{
			counts: map[string]int64{model.EventOutboxStatusDispatched: 10},
			audit:  audit,
		}
		offsetCalls := 0

		statistics, err := eventOutboxStatistics(context.Background(), store,
			statsOffsetReader(statsMeasuredReport(), nil, &offsetCalls), EventOffsetsBestEffort, 0)
		require.NoError(t, err)

		assert.True(t, statistics.AuditRead)
		assert.True(t, statistics.OffsetsRead)
		require.NotNil(t, statistics.Reconciliation)

		// Delegated to ReconcileAgainstOutbox rather than recomputed, so the comparison
		// stays directional: records are a lower bound on events, and only a shortfall is
		// evidence of loss.
		assert.Equal(t, ReconcileAgainstOutbox(statistics.Offsets, audit), *statistics.Reconciliation)
		assert.False(t, statistics.Reconciliation.LossDetected)
	})

	t.Run("a measurement covering no topics yields no verdict", func(t *testing.T) {
		store := &statsFakeStore{
			counts: map[string]int64{model.EventOutboxStatusDispatched: 10},
			audit:  audit,
		}
		offsetCalls := 0

		statistics, err := eventOutboxStatistics(context.Background(), store,
			statsOffsetReader(TopicOffsetReport{MeasuredAt: time.Now().UTC()}, nil, &offsetCalls),
			EventOffsetsRequired, 0)
		require.NoError(t, err)

		assert.True(t, statistics.OffsetsRead, "the read itself succeeded")
		assert.Nil(t, statistics.Reconciliation,
			"with no topics covered there is nothing to compare, so a verdict would assert something it never established")
	})
}

// TestEventOutboxStatistics_ComparesOneCommonPopulation is the V-2 requirement that the
// two sides of the zero-loss check describe the same interval.
//
// The outbox side has always been windowed — it must be, since dispatched rows accumulate
// without bound — while the broker side was read cumulatively: every record the topics had
// ever accepted. Comparing those two produces a surplus that grows for the life of the
// topic, and worse, it switches OFF the only arithmetic that can detect loss, because a
// shortfall of records against rows is only meaningful when both counts cover one interval.
// The reading could still call itself conclusive.
//
// Both halves are asserted here: that the same instant reaches the broker reader, and that a
// reading which nonetheless comes back unwindowed refuses to conclude.
func TestEventOutboxStatistics_ComparesOneCommonPopulation(t *testing.T) {
	t.Run("the broker is measured from the instant the outbox was counted from", func(t *testing.T) {
		store := &statsFakeStore{counts: map[string]int64{model.EventOutboxStatusDispatched: 3}}

		var offsetWindow time.Time

		report := statsMeasuredReport()

		statistics, err := eventOutboxStatistics(
			context.Background(), store,
			statsWindowRecordingOffsetReader(report, &offsetWindow),
			EventOffsetsBestEffort, 6*time.Hour,
		)
		require.NoError(t, err)

		require.False(t, offsetWindow.IsZero(),
			"the broker read must be bounded; a zero instant is the cumulative reading V-2 cannot use")
		assert.Equal(t, statistics.WindowStart, offsetWindow,
			"one window start, derived once, reaching both sides — otherwise the verdict compares "+
				"two populations")
		assert.Equal(t, store.countedSince, offsetWindow,
			"the outbox count and the broker read must name the same instant")
		assert.Equal(t, 6*time.Hour, statistics.Window,
			"the honoured window is reported so a reader can see what the figures cover")
	})

	t.Run("an unwindowed reading never calls a cumulative total a surplus", func(t *testing.T) {
		// A broker report carrying no window start is the cumulative reading: every record the
		// topics have ever accepted. The verdict is still produced, and it can still be green —
		// what establishes it is the per-row coordinate mapping, not the totals — but the DIFFERENCE
		// between the cumulative total and the row count is not a surplus over anything, and the
		// green sentence used to report it as one. On a long-lived topic that put a number in the
		// millions in front of an operator as though it were unaccounted copies.
		cumulative := statsMeasuredReport()
		cumulative.WindowStart = time.Time{}

		store := &statsFakeStore{
			counts: map[string]int64{model.EventOutboxStatusDispatched: 10},
			audit: model.EventRecordIntervalAudit{
				PublishedRows:               10,
				CorroboratedRows:            10,
				DistinctCorroboratedRecords: 10,
			},
		}
		offsetCalls := 0

		statistics, err := eventOutboxStatistics(context.Background(), store,
			statsOffsetReader(cumulative, nil, &offsetCalls), EventOffsetsBestEffort, 0)
		require.NoError(t, err)
		require.NotNil(t, statistics.Reconciliation)

		assert.False(t, statistics.Reconciliation.Windowed,
			"the flag is what a reader branches on, so it must report the truth about the reading")

		summary := statistics.Reconciliation.Summary()
		assert.Contains(t, summary, "CUMULATIVE",
			"the reading's scope must be stated in the sentence an operator reads")
		assert.NotContains(t, summary, "surplus are redelivery",
			"the windowed green sentence's surplus claim must not be used for a cumulative total")

		// AND THE SHORTFALL ARITHMETIC STAYS OFF. It is the signal that rows claim a publication
		// that never happened, and over two different intervals it produces nothing but noise.
		assert.False(t, statistics.Reconciliation.LossDetected)
	})
}

// TestEventOutboxStatistics_ReportsTheEventsThatAreOwed covers the two censuses that no
// per-status count can reveal.
//
// A monitor alert does not exist until the balance is committed, and a bulk batch's summary
// belongs to no single member transaction, so neither is captured inside the transaction
// that produces it. Each has an intent written atomically instead. An outstanding intent is
// an event that is OWED with no outbox row for it yet — so a reconciliation reading only the
// outbox reports a clean pipeline while alerts and batch summaries are still pending.
//
// The distinction that matters is between MEASURED-AND-ZERO and NOT-MEASURED. Reporting
// zeros for a census that could not be read is the one misreading a zero-loss check cannot
// afford, which is why the field is a pointer and why it is nil rather than zero on failure.
func TestEventOutboxStatistics_ReportsTheEventsThatAreOwed(t *testing.T) {
	oldest := time.Now().UTC().Add(-2 * time.Hour)

	t.Run("the census travels with the counts", func(t *testing.T) {
		store := &statsFakeStore{
			counts: map[string]int64{model.EventOutboxStatusDispatched: 1},
			handoff: map[string]int64{
				model.OutboxStatusPending: 2,
				model.OutboxStatusFailed:  1,
			},
			batches:   3,
			batchesAt: &oldest,
		}

		statistics, err := eventOutboxStatistics(
			context.Background(), store, nil, EventOffsetsSkipped, 0,
		)
		require.NoError(t, err)
		require.NotNil(t, statistics.ProducerAtomicity,
			"the field is declared on the response, so leaving it unset promises a figure and "+
				"delivers nothing")

		assert.Equal(t, int64(2), statistics.ProducerAtomicity.MonitorHandoffPending)
		assert.Equal(t, int64(1), statistics.ProducerAtomicity.MonitorHandoffFailed)
		assert.Zero(t, statistics.ProducerAtomicity.MonitorHandoffProcessing,
			"a state with no rows is absent from the aggregate and reads as zero, which is correct")
		assert.Equal(t, int64(3), statistics.ProducerAtomicity.UnfinalizedBatches)
		require.NotNil(t, statistics.ProducerAtomicity.OldestUnfinalizedBatchAt)
		assert.WithinDuration(t, oldest, *statistics.ProducerAtomicity.OldestUnfinalizedBatchAt, 0)

		// READ IN EVERY POSTURE, including the one that never touches Kafka. Both censuses
		// come from PostgreSQL, and a skipped or unreachable broker is exactly when an
		// operator is asking what is outstanding.
		assert.Equal(t, 1, store.censusAttempts)
	})

	t.Run("a census that cannot be read is absent rather than zero", func(t *testing.T) {
		// PENDING rather than dispatched, because the posture below is Skipped and a skipped
		// posture reads the UNRESOLVED aggregate, which has no dispatched arm at all
		// (PERF-M05). A dispatched-only fixture would come back empty and the "the counts
		// still answer" assertion below would then be checking the wrong thing.
		for name, store := range map[string]*statsFakeStore{
			"the handoff census fails": {
				counts:     map[string]int64{model.EventOutboxStatusPending: 1},
				handoffErr: errors.New("dial tcp 10.0.0.7:5432: connect: refused"),
			},
			"the batch census fails": {
				counts:     map[string]int64{model.EventOutboxStatusPending: 1},
				batchesErr: errors.New("statement timeout"),
			},
		} {
			t.Run(name, func(t *testing.T) {
				statistics, err := eventOutboxStatistics(
					context.Background(), store, nil, EventOffsetsSkipped, 0,
				)

				// THE COUNTS STILL ANSWER. The census is an enrichment: losing it must not
				// take the whole reading down, or a slow query on one table hides every figure.
				require.NoError(t, err)
				assert.NotEmpty(t, statistics.CountsByStatus)
				assert.Nil(t, statistics.ProducerAtomicity,
					"zeros would say nothing is owed, which is the opposite of what is known")
			})
		}
	})
}

// TestEventOutboxStatistics_NamesAStatusNothingReportsACountFor closes the gap that
// makes a short total indistinguishable from a lost event.
//
// The status column deliberately permits values the code has not learned yet, so the
// state machine can be extended without a migration. A new state therefore appears in
// the aggregate before any consumer's shape learns about it, and its rows are then
// missing from every reported total — which is exactly what a zero-loss reconciliation
// cannot tolerate. Naming it is the cheapest thing that makes the gap visible.
func TestEventOutboxStatistics_NamesAStatusNothingReportsACountFor(t *testing.T) {
	store := &statsFakeStore{counts: map[string]int64{
		model.EventOutboxStatusDispatched: 4,
		"quarantined":                     2,
		"archived":                        1,
	}}

	statistics, err := eventOutboxStatistics(
		context.Background(), store, nil, EventOffsetsSkipped, 0,
	)
	require.NoError(t, err)

	assert.Equal(t, []string{"archived", "quarantined"}, statistics.UnreportedStatuses,
		"unknown statuses must be named, and sorted so the warning and any assertion on it are deterministic")

	t.Run("every known status is reported as known", func(t *testing.T) {
		counts := make(map[string]int64, len(model.EventOutboxStatuses()))
		for _, status := range model.EventOutboxStatuses() {
			counts[status] = 1
		}

		statistics, err := eventOutboxStatistics(
			context.Background(), &statsFakeStore{counts: counts}, nil, EventOffsetsSkipped, 0,
		)
		require.NoError(t, err)
		assert.Empty(t, statistics.UnreportedStatuses,
			"the whole state machine must be recognised, or the enumeration and the table have drifted")
	})

	t.Run("an empty table names nothing", func(t *testing.T) {
		statistics, err := eventOutboxStatistics(
			context.Background(), &statsFakeStore{counts: map[string]int64{}}, nil, EventOffsetsSkipped, 0,
		)
		require.NoError(t, err)
		assert.Empty(t, statistics.UnreportedStatuses)
	})
}

// TestReconcileSubscriberACLs_LeavesEveryBindingBlnkDoesNotOwn is the safety half of AUTH-02.
//
// ACL deletion has no undo, and a reconciliation wide enough to tidy an operator's deliberate
// work is wide enough to delete something load-bearing that nobody remembers creating. So the
// ownership test is an ALLOWLIST of the two shapes Blnk provisions, and everything else on the
// principal is reported and left exactly where it is.
//
// # Why every fixture here is a DENY
//
// It used to mix DENY with three ALLOW shapes — a deliberate Write, a prefixed topic pattern and
// a cluster Describe — and require NO error. AUTH-03 withdrew that: a foreign ALLOW makes the
// principal's effective access broader than the registry records, so provisioning now REFUSES
// rather than issuing a credential whose stated boundary the broker is not enforcing, and
// TestProvisionSubscriberPrincipal_RefusesAForeignAllowBinding exercises all four of those shapes
// on their own. Keeping them here asserted the opposite answer for the same input.
//
// What this test still proves, and what its sibling does not, is that ownership is decided by
// SHAPE across every dimension: resource type, pattern type and operation. Every fixture is a
// DENY so that none of them can trip AUTH-03 — a DENY only ever subtracts from what the ALLOW
// bindings grant, so it cannot widen effective access and reconciliation carries on past it.
func TestReconcileSubscriberACLs_LeavesEveryBindingBlnkDoesNotOwn(t *testing.T) {
	storeKafkaTopicPrefix(t, DefaultTopicPrefix)

	principal := testSubscriberPrincipal(t)

	foreign := []kafka.ACLEntry{
		{
			// A DENY on a topic Blnk DOES grant literally. Removing it would WIDEN access,
			// which is the worst possible direction.
			ResourceType:        kafka.ResourceTypeTopic,
			ResourceName:        "blnk.transactions",
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           principal,
			Host:                "*",
			Operation:           kafka.ACLOperationTypeRead,
			PermissionType:      kafka.ACLPermissionTypeDeny,
		},
		{
			// A DENY on an operation Blnk never grants at all. Blnk provisions Read and
			// Describe, so a Write binding cannot be its work whichever way it points.
			ResourceType:        kafka.ResourceTypeTopic,
			ResourceName:        "blnk.transactions",
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           principal,
			Host:                "*",
			Operation:           kafka.ACLOperationTypeWrite,
			PermissionType:      kafka.ACLPermissionTypeDeny,
		},
		{
			// A PREFIXED pattern. Blnk grants topics literally, so a prefixed binding is
			// somebody else's regardless of which way it points.
			ResourceType:        kafka.ResourceTypeTopic,
			ResourceName:        "blnk.",
			ResourcePatternType: kafka.PatternTypePrefixed,
			Principal:           principal,
			Host:                "*",
			Operation:           kafka.ACLOperationTypeRead,
			PermissionType:      kafka.ACLPermissionTypeDeny,
		},
		{
			// A cluster resource. Outside the two resource types Blnk provisions entirely.
			ResourceType:        kafka.ResourceTypeCluster,
			ResourceName:        "kafka-cluster",
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           principal,
			Host:                "*",
			Operation:           kafka.ACLOperationTypeDescribe,
			PermissionType:      kafka.ACLPermissionTypeDeny,
		},
	}

	fake := newFakeAdminClient()
	for _, binding := range foreign {
		fake.withBinding(binding)
	}

	// And one genuinely stale Blnk-shaped binding, so the test proves reconciliation still
	// does its job rather than passing by doing nothing at all.
	stale := topicBinding(t, "blnk.identities", kafka.ACLOperationTypeRead)
	fake.withBinding(stale)

	admin := newTestKafkaAdmin(fake, MinTopicPartitions, 1)

	report, err := admin.ProvisionSubscriberPrincipal(
		context.Background(),
		NewSubscriberProvisioningRequest(testSubscriber(), sentinelPassword),
	)
	require.NoError(t, err,
		"none of these bindings grants access, so none of them can make the effective access "+
			"broader than the registry records and issuance must proceed")

	assert.Equal(t, 1, report.ACLBindingsRemoved,
		"exactly the one stale Blnk-shaped binding may be removed")
	assert.Equal(t, len(foreign), report.ForeignACLBindings,
		"every binding outside Blnk's ownership must be REPORTED so an operator can decide")
	assert.Zero(t, report.ForeignACLBindingsGranting,
		"and none of them grants, which is what makes carrying on past them safe")

	held := aclKeys(fake.heldBindings())
	for _, binding := range foreign {
		assert.Contains(t, held, fakeACLKey(binding),
			"a binding Blnk does not provision must survive reconciliation untouched")
	}
	assert.NotContains(t, held, fakeACLKey(stale),
		"the stale Blnk-shaped binding must be gone")
}

// catalogueGateStub drives TopicCatalogueGate without a broker.
type catalogueGateStub struct {
	mu      sync.Mutex
	calls   int
	reports []TopicCatalogueReport
	errs    []error
}

func (s *catalogueGateStub) verify(context.Context) (TopicCatalogueReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	index := s.calls
	s.calls++

	// The last entry repeats, so a test states the interesting prefix of the sequence and the
	// steady state that follows it.
	if index >= len(s.reports) {
		index = len(s.reports) - 1
	}

	var err error
	if index < len(s.errs) {
		err = s.errs[index]
	}

	return s.reports[index], err
}

func (s *catalogueGateStub) probeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls
}

// completeCatalogue is a report saying every expected topic exists.
func completeCatalogue() TopicCatalogueReport {
	return TopicCatalogueReport{Expected: AllOwnedTopicsAcrossPrefixes(), VerifiedAt: relayFixedNow}
}

// incompleteCatalogue is a report saying one dead-letter topic is absent — the case that is
// worst in practice, because it fails the PRESERVATION of an event whose budget is spent.
func incompleteCatalogue() TopicCatalogueReport {
	return TopicCatalogueReport{
		Expected:   AllOwnedTopicsAcrossPrefixes(),
		Missing:    []string{DLTFor(TopicForCategory(model.EventCategoryTransactions))},
		VerifiedAt: relayFixedNow,
	}
}

// TestTopicCatalogueGate_OpensOnceAndOnlyOnAVerifiedCatalogue is the gate's contract.
//
// # What the gate is for
//
// Every write the relay makes goes to a Blnk-owned topic and auto-creation is disabled, so a
// missing topic fails the publish, spends the row's retry budget one attempt at a time, and
// then fails the dead-letter write for the same reason — leaving the row failed with no
// dead-letter topic recorded. A boot against an unprovisioned broker could do that to every
// pending row. The gate is what stops the relay claiming a row it cannot deliver.
//
// # Why "opens once" matters as much as "opens only when verified"
//
// The gate is consulted on every poll — once a second at the defaults. If it probed the broker
// each time, the fix for one failure mode would be a permanent metadata read on the relay's
// path, plus a log line per second for as long as a broker stayed unprovisioned. Latching
// success and backing off failure is what makes it affordable enough to be consulted at all.
func TestTopicCatalogueGate_OpensOnceAndOnlyOnAVerifiedCatalogue(t *testing.T) {
	t.Run("a verified catalogue opens the gate and is never re-probed", func(t *testing.T) {
		stub := &catalogueGateStub{reports: []TopicCatalogueReport{completeCatalogue()}}
		gate := &TopicCatalogueGate{
			verify:        stub.verify,
			now:           func() time.Time { return relayFixedNow },
			probeInterval: defaultCatalogueProbeInterval,
		}

		require.NoError(t, gate.Ready(context.Background()))
		assert.True(t, gate.Verified())

		for i := 0; i < 25; i++ {
			require.NoError(t, gate.Ready(context.Background()))
		}
		assert.Equal(t, 1, stub.probeCount(),
			"success must LATCH: the relay consults the gate every poll, so re-probing would put a "+
				"broker round trip on that path for the life of the process")
	})

	t.Run("missing topics keep the gate shut and name them", func(t *testing.T) {
		stub := &catalogueGateStub{reports: []TopicCatalogueReport{incompleteCatalogue()}}
		gate := &TopicCatalogueGate{
			verify:        stub.verify,
			now:           func() time.Time { return relayFixedNow },
			probeInterval: defaultCatalogueProbeInterval,
		}

		err := gate.Ready(context.Background())
		require.Error(t, err, "an incomplete catalogue must refuse: publishing to a topic that does "+
			"not exist spends the row's retry budget and then cannot dead-letter either")
		assert.ErrorIs(t, err, errCatalogueIncomplete)
		assert.Contains(t, err.Error(), DLTFor(TopicForCategory(model.EventCategoryTransactions)),
			"the refusal must name the absent topic, or an operator has nothing to act on")
		assert.False(t, gate.Verified())
	})

	t.Run("a broker that cannot be reached refuses without claiming to be verified", func(t *testing.T) {
		unreachable := errors.New("dial tcp 10.0.0.1:9092: i/o timeout")
		stub := &catalogueGateStub{
			reports: []TopicCatalogueReport{{Expected: AllOwnedTopicsAcrossPrefixes()}},
			errs:    []error{unreachable},
		}
		gate := &TopicCatalogueGate{
			verify:        stub.verify,
			now:           func() time.Time { return relayFixedNow },
			probeInterval: defaultCatalogueProbeInterval,
		}

		err := gate.Ready(context.Background())
		require.Error(t, err)
		assert.ErrorIs(t, err, unreachable,
			"an UNKNOWN catalogue must not be treated as an incomplete one or as a verified one: "+
				"the relay's decision is the same, but the reason an operator is given is not")
		assert.False(t, gate.Verified())
	})

	t.Run("probes are rate-limited between attempts and back off", func(t *testing.T) {
		stub := &catalogueGateStub{reports: []TopicCatalogueReport{incompleteCatalogue()}}
		now := relayFixedNow
		gate := &TopicCatalogueGate{
			verify:        stub.verify,
			now:           func() time.Time { return now },
			probeInterval: defaultCatalogueProbeInterval,
		}

		require.Error(t, gate.Ready(context.Background()))
		require.Equal(t, 1, stub.probeCount())

		// Every tick inside the window is answered from the gate's own state, with no broker
		// contact and no second log line.
		for i := 0; i < 10; i++ {
			err := gate.Ready(context.Background())
			require.ErrorIs(t, err, errCatalogueNotVerified,
				"inside the backoff window the refusal is silent and free")
		}
		assert.Equal(t, 1, stub.probeCount(), "the window must suppress the probe, not just the log")

		// Past the window it probes again, and the window has doubled.
		now = now.Add(defaultCatalogueProbeInterval)
		require.Error(t, gate.Ready(context.Background()))
		assert.Equal(t, 2, stub.probeCount())
		assert.Equal(t, defaultCatalogueProbeInterval*4, gate.probeInterval,
			"the interval doubles on each failed probe so a long outage is not interrogated once a "+
				"second, and it is read AFTER the second failure has grown it twice")
	})

	t.Run("the backoff is capped so recovery never needs a restart", func(t *testing.T) {
		stub := &catalogueGateStub{reports: []TopicCatalogueReport{incompleteCatalogue()}}
		now := relayFixedNow
		gate := &TopicCatalogueGate{
			verify:        stub.verify,
			now:           func() time.Time { return now },
			probeInterval: defaultCatalogueProbeInterval,
		}

		for i := 0; i < 20; i++ {
			require.Error(t, gate.Ready(context.Background()))
			now = now.Add(maxCatalogueProbeInterval)
		}

		assert.Equal(t, maxCatalogueProbeInterval, gate.probeInterval,
			"an unbounded backoff would eventually make automatic recovery indistinguishable from "+
				"a restart, which is the very thing gating instead of refusing to start avoids")
	})

	t.Run("a broker that comes good later opens the gate without a restart", func(t *testing.T) {
		stub := &catalogueGateStub{reports: []TopicCatalogueReport{
			incompleteCatalogue(),
			completeCatalogue(),
		}}
		now := relayFixedNow
		gate := &TopicCatalogueGate{
			verify:        stub.verify,
			now:           func() time.Time { return now },
			probeInterval: defaultCatalogueProbeInterval,
		}

		require.Error(t, gate.Ready(context.Background()))

		now = now.Add(defaultCatalogueProbeInterval)
		require.NoError(t, gate.Ready(context.Background()),
			"this is the whole reason the relay starts rather than refusing: provisioning the topics "+
				"must be enough, with no human restarting anything")
		assert.True(t, gate.Verified())
	})

	t.Run("a nil gate and a verifierless gate are both handled", func(t *testing.T) {
		var absent *TopicCatalogueGate
		assert.NoError(t, absent.Ready(context.Background()),
			"a nil gate is ungated, which is the pre-existing behaviour every test relies on")
		assert.False(t, absent.Verified())

		empty := &TopicCatalogueGate{now: func() time.Time { return relayFixedNow }}
		assert.Error(t, empty.Ready(context.Background()),
			"a gate that cannot verify anything must refuse rather than open by default")
	})
}

// reconciliationWindowStart is the instant both sides of a reconciliation are measured from.
//
// A CONCLUSIVE verdict now requires the two sides to name the same window (PERF-P05), so a
// fixture that set one on neither would be testing the not-windowed caveat rather than the
// direction of the comparison. Fixed rather than time.Now() so both sides agree exactly, which
// is the property the verdict checks.
func reconciliationWindowStart() time.Time {
	return time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)
}

// windowedOffsetReport builds a broker-side report measured over the shared window, in which
// `records` were written inside it.
//
// EndOffsetSum and WindowRecordCount are set to the same figure because these fixtures describe
// a topic whose whole content falls inside the window; RetainedCount matches so nothing looks
// truncated. The verdict reads WindowRecordCount when both sides are windowed, so that is the
// figure that decides the comparison.
func windowedOffsetReport(records int64) TopicOffsetReport {
	return TopicOffsetReport{
		EndOffsetSum:      records,
		RetainedCount:     records,
		WindowRecordCount: records,
		WindowStart:       reconciliationWindowStart(),
		MeasuredAt:        time.Now().UTC(),
	}
}

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
		report := windowedOffsetReport(1_100)

		verdict := ReconcileAgainstOutbox(report, corroboratedAudit(1_000, auditBoundedBySharedWindow))
		assert.False(t, verdict.LossDetected,
			"100 more records than events is redelivery and replay overhead, which is normal")
		assert.True(t, verdict.Conclusive)
		assert.True(t, verdict.Windowed,
			"both sides named the same window, which is what makes the comparison a comparison")
		assert.Equal(t, int64(100), verdict.Overhead)
		assert.Contains(t, verdict.Summary(), "NO LOSS DETECTED")
		assert.Contains(t, verdict.Summary(), "names the distinct broker record it produced",
			"a green verdict must state WHY the surplus cannot be masking loss, not merely that "+
				"none was detected")
	})

	t.Run("a shortfall is loss", func(t *testing.T) {
		report := windowedOffsetReport(990)

		verdict := ReconcileAgainstOutbox(report, corroboratedAudit(1_000, auditBoundedBySharedWindow))
		assert.True(t, verdict.LossDetected,
			"fewer records than rows means rows claim a publication that never happened")
		assert.Equal(t, int64(-10), verdict.Overhead,
			"the shortfall must stay signed; clamping it would erase the only signal here")
		assert.Contains(t, verdict.Summary(), "LOSS DETECTED")
	})

	t.Run("exact equality is not loss", func(t *testing.T) {
		report := windowedOffsetReport(1_000)

		verdict := ReconcileAgainstOutbox(report, corroboratedAudit(1_000, auditBoundedBySharedWindow))
		assert.False(t, verdict.LossDetected, "the boundary is inclusive: equal is not short")
		assert.Zero(t, verdict.Overhead)
	})
}

// TestEventOutboxAudit_DerivesItsOwnConclusions pins the two derived answers, because both are
// read as guards and an off-by-one in either would change a verdict.
func TestEventOutboxAudit_DerivesItsOwnConclusions(t *testing.T) {
	t.Run("unconfirmed rows never go negative", func(t *testing.T) {
		// ConfirmedRows can only exceed PublishedRows if the two counts were read from
		// different queries, but a negative "unconfirmed" would be reported as a caveat and
		// send an operator looking for rows that do not exist.
		audit := model.EventOutboxAudit{PublishedRows: 10, ConfirmedRows: 12, DistinctRecords: 12}
		assert.Zero(t, audit.UnconfirmedRows())
	})

	t.Run("fully confirmed requires both properties", func(t *testing.T) {
		assert.True(t, model.EventOutboxAudit{
			PublishedRows: 5, ConfirmedRows: 5, DistinctRecords: 5,
		}.FullyConfirmed())

		assert.False(t, model.EventOutboxAudit{
			PublishedRows: 5, ConfirmedRows: 4, DistinctRecords: 4,
		}.FullyConfirmed(), "an unconfirmed row is enough to disqualify it")

		assert.False(t, model.EventOutboxAudit{
			PublishedRows: 5, ConfirmedRows: 5, DistinctRecords: 4,
		}.FullyConfirmed(), "so is a shared coordinate")
	})

	t.Run("nothing published is fully confirmed", func(t *testing.T) {
		assert.True(t, model.EventOutboxAudit{}.FullyConfirmed())
	})
}

// TestValidateDesiredACLBindings_RefusesEveryShapeThatWidensAGrant is the M-1 guard on the
// bindings Blnk is about to CREATE, as opposed to the ones it reads back.
//
// blnkManagedACLBinding is the ownership allowlist, and reconciliation already applied it to what
// the broker REPORTS. That caught a widened binding one reconcile too late: the binding was
// written, it was live, and only the NEXT reconcile classified it as a foreign ALLOW and refused —
// so the failure mode of a widening bug was "grant the access, then jam this subscriber's
// provisioning permanently". Checking the desired set inverts that. An ACL mistake has no runtime
// symptom on the Blnk side, so a guard that runs before the write is the only one that prevents
// rather than reports.
func TestValidateDesiredACLBindings_RefusesEveryShapeThatWidensAGrant(t *testing.T) {
	const principal = "blnk-sub-acme"

	owned := func(mutate func(*kafka.ACLEntry)) kafka.ACLEntry {
		entry := kafka.ACLEntry{
			ResourceType:        kafka.ResourceTypeTopic,
			ResourceName:        "blnk.transactions",
			ResourcePatternType: kafka.PatternTypeLiteral,
			Principal:           kafkaPrincipalPrefix + principal,
			Host:                ACLHostAny,
			Operation:           kafka.ACLOperationTypeRead,
			PermissionType:      kafka.ACLPermissionTypeAllow,
		}
		if mutate != nil {
			mutate(&entry)
		}

		return entry
	}

	t.Run("the two owned shapes are accepted", func(t *testing.T) {
		group := owned(func(e *kafka.ACLEntry) {
			e.ResourceType = kafka.ResourceTypeGroup
			e.ResourceName = "blnk-sub-acme."
			e.ResourcePatternType = kafka.PatternTypePrefixed
		})
		describe := owned(func(e *kafka.ACLEntry) { e.Operation = kafka.ACLOperationTypeDescribe })

		require.NoError(t, validateDesiredACLBindings(principal,
			[]kafka.ACLEntry{owned(nil), describe, group}),
			"the guard must accept exactly what aclEntries produces, or it blocks provisioning")
	})

	t.Run("an empty desired set is a legitimate instruction", func(t *testing.T) {
		// "Authorised for nothing" is the fail-closed default of a fresh registration, and
		// reconciling to it is how a cleared topic list reaches the broker. Refusing it here
		// would make a narrowing to empty impossible to apply.
		require.NoError(t, validateDesiredACLBindings(principal, nil))
	})

	for name, binding := range map[string]kafka.ACLEntry{
		// THE dangerous edit. A PREFIXED topic pattern converts "Read blnk.transactions" into
		// "Read every topic whose name starts with blnk.transactions" — which includes
		// blnk.transactions.dlt, every failed event on the cluster.
		"a prefixed topic pattern": owned(func(e *kafka.ACLEntry) {
			e.ResourcePatternType = kafka.PatternTypePrefixed
		}),
		// Kafka reads the resource name "*" as matching EVERY resource, so this passes a naive
		// shape check and is a cluster-wide grant.
		"the wildcard as a topic name": owned(func(e *kafka.ACLEntry) { e.ResourceName = "*" }),
		"a blank topic name":           owned(func(e *kafka.ACLEntry) { e.ResourceName = "" }),
		// Untrimmed names are refused rather than trimmed: Kafka would treat the padded form as
		// a different topic from the one that reads correctly here.
		"a padded topic name": owned(func(e *kafka.ACLEntry) { e.ResourceName = " blnk.transactions" }),
		// Blnk grants Read and Describe and nothing else. Write on a ledger topic would let a
		// subscriber forge events.
		"a write operation": owned(func(e *kafka.ACLEntry) { e.Operation = kafka.ACLOperationTypeWrite }),
		"a cluster resource": owned(func(e *kafka.ACLEntry) {
			e.ResourceType = kafka.ResourceTypeCluster
		}),
		// A literal group pattern reserves one group id instead of the namespace, so the
		// subscriber cannot join any other group of its own — and a prefixed TOPIC is the
		// mirror-image mistake.
		"a literal group pattern": owned(func(e *kafka.ACLEntry) {
			e.ResourceType = kafka.ResourceTypeGroup
			e.ResourceName = "blnk-sub-acme."
			e.ResourcePatternType = kafka.PatternTypeLiteral
		}),
		// Blnk never provisions a Deny, so one appearing in a desired set means the set was
		// assembled by something other than aclEntries.
		"a deny binding": owned(func(e *kafka.ACLEntry) {
			e.PermissionType = kafka.ACLPermissionTypeDeny
		}),
		// A binding carrying another identity would grant this subscriber's topics to a
		// different credential, and reconciliation would never remove it: it describes by
		// principal.
		"another principal": owned(func(e *kafka.ACLEntry) {
			e.Principal = kafkaPrincipalPrefix + "blnk-sub-other"
		}),
		"an unprefixed principal": owned(func(e *kafka.ACLEntry) { e.Principal = principal }),
	} {
		t.Run("refuses "+name, func(t *testing.T) {
			err := validateDesiredACLBindings(principal, []kafka.ACLEntry{binding})

			require.Error(t, err, "this binding widens or misdirects the grant and must not be written")
			assert.ErrorIs(t, err, ErrSubscriberBindingShapeUnsupported,
				"the refusal must be classifiable, so a caller can tell it from a broker failure")
		})
	}

	t.Run("a widened binding among valid ones is still caught", func(t *testing.T) {
		// The guard must scan the whole set. Stopping at the first owned binding is the bug that
		// would let a widened entry ride along behind a correct one.
		err := validateDesiredACLBindings(principal, []kafka.ACLEntry{
			owned(nil),
			owned(func(e *kafka.ACLEntry) { e.ResourcePatternType = kafka.PatternTypePrefixed }),
		})

		require.Error(t, err)
		assert.ErrorIs(t, err, ErrSubscriberBindingShapeUnsupported)
	})

	t.Run("what aclEntries actually produces passes the guard", func(t *testing.T) {
		// The guard and the builder must agree, and they are separate code. If they ever
		// disagree, every provisioning fails closed — safe, but a total outage — so this pins
		// the agreement rather than trusting it.
		row := model.EventSubscriber{
			SubscriberID:     "sub_" + strings.Repeat("a", 20),
			AuthorizedTopics: []string{"blnk.transactions", "blnk.balances"},
			ConsumerGroupID:  "grp",
		}
		request := NewSubscriberProvisioningRequest(&row, sentinelPassword)

		require.NoError(t, validateDesiredACLBindings(request.boundPrincipal(), request.aclEntries()),
			"aclEntries is the only producer of desired bindings, so its output must satisfy the "+
				"guard every write path runs it through")
	})
}

// TestCreateACLBindings_IsTheUnbypassableGate proves the shape guard sits on the ONE function that
// writes, rather than on a caller.
//
// This test exists because the guard was first installed in reconcileSubscriberACLs, and that was
// wrong in a way only a caller census reveals: THREE functions create bindings — provisioning's
// reconciliation, GrantSubscriberAccess/PruneSubscriberAccess, and the exported
// ReconcileSubscriberACLs that applies an edited topic list. A guard on one of them leaves the
// other two open, and the one it left open was the grant-edit path, which is the path an operator
// uses most. So the assertion here is not "a widened binding is refused" — the unit test above
// covers that — it is "the refusal happens without the broker being called at all", which is only
// true if the check precedes the CreateACLs round trip inside the writer itself.
func TestCreateACLBindings_IsTheUnbypassableGate(t *testing.T) {
	const principal = "blnk-sub-acme"

	widened := kafka.ACLEntry{
		ResourceType: kafka.ResourceTypeTopic,
		ResourceName: "blnk.transactions",
		// The dangerous shape: PREFIXED widens this to every topic starting with the name,
		// blnk.transactions.dlt included.
		ResourcePatternType: kafka.PatternTypePrefixed,
		Principal:           kafkaPrincipalPrefix + principal,
		Host:                ACLHostAny,
		Operation:           kafka.ACLOperationTypeRead,
		PermissionType:      kafka.ACLPermissionTypeAllow,
	}

	fake := newFakeAdminClient()
	admin := newTestKafkaAdmin(fake, 6, 1)

	err := admin.createACLBindings(context.Background(), principal, []kafka.ACLEntry{widened})

	require.Error(t, err, "the writer itself must refuse a binding outside the two owned shapes")
	assert.ErrorIs(t, err, ErrSubscriberBindingShapeUnsupported)

	// THE POINT OF THIS TEST. An ACL mistake has no runtime symptom, so a guard that refuses
	// after the write has already landed prevents nothing.
	assert.Empty(t, fake.createACLsRequests,
		"the broker must not have been asked to create anything; a guard that runs after "+
			"CreateACLs would leave the widened binding live and merely report it")
	assert.Empty(t, fake.bindings,
		"no binding may exist in the broker's store after a refused create")
}
