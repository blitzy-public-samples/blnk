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

	"github.com/blnkfinance/blnk/internal/metrics"
	"github.com/blnkfinance/blnk/model"
)

// clock returns the time source, defaulting to time.Now.
func (a *KafkaAdminClient) clock() time.Time {
	if a.now != nil {
		return a.now()
	}

	return time.Now()
}

// snapshotTTL returns the effective cache lifetime.
func (a *KafkaAdminClient) snapshotTTL() time.Duration {
	if a.offsetSnapshotTTL == 0 {
		return defaultOffsetSnapshotTTL
	}

	return a.offsetSnapshotTTL
}

// WithOffsetSnapshotTTL sets how long a partition/end-offset snapshot may be reused.
//
// Parameters:
//   - ttl time.Duration: the lifetime. Negative disables caching, zero restores the
//     default.
//
// Returns:
//   - *KafkaAdminClient: the client, for chaining.
func (a *KafkaAdminClient) WithOffsetSnapshotTTL(ttl time.Duration) *KafkaAdminClient {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()

	a.offsetSnapshotTTL = ttl
	a.offsetSnapshots = nil
	a.authorizerProbe = cachedAuthorizerProbe{}

	return a
}

// InvalidateOffsetSnapshot drops every cached snapshot and the authorizer probe.
func (a *KafkaAdminClient) InvalidateOffsetSnapshot() {
	a.cacheMu.Lock()
	defer a.cacheMu.Unlock()

	a.offsetSnapshots = nil
	a.authorizerProbe = cachedAuthorizerProbe{}
}

// offsetSnapshotKey builds the cache key for a topic set.
func offsetSnapshotKey(topics []string) string {
	return strings.Join(topics, "\x00")
}

// partitionOffsetSnapshot returns the partition layout and end offsets for a topic set,
// from cache when it is fresh.
func (a *KafkaAdminClient) partitionOffsetSnapshot(
	ctx context.Context,
	topics []string,
) (*offsetSnapshot, error) {
	ttl := a.snapshotTTL()
	key := offsetSnapshotKey(topics)

	if ttl > 0 {
		a.cacheMu.Lock()
		cached, found := a.offsetSnapshots[key]
		a.cacheMu.Unlock()

		if found && a.clock().Sub(cached.takenAt) < ttl {
			return cached, nil
		}
	}

	partitions, err := a.topicPartitions(ctx, topics)
	if err != nil {
		// The partial result is still useful to the caller: it reports which topics were
		// missing, which is a caveat the lag report carries even on failure.
		return &offsetSnapshot{
			partitions: partitions,
			missing:    missingTopics(topics, partitions),
			takenAt:    a.clock(),
		}, err
	}

	snapshot := &offsetSnapshot{
		partitions: partitions,
		missing:    missingTopics(topics, partitions),
		takenAt:    a.clock(),
	}

	if len(partitions) > 0 {
		bounds, boundsErr := a.offsetBounds(ctx, partitions)
		if boundsErr != nil {
			return snapshot, boundsErr
		}
		snapshot.bounds = bounds
	}

	if ttl > 0 {
		a.cacheMu.Lock()
		// Dropped wholesale at the bound rather than evicted entry by entry: entries live for
		// seconds, so the next sweep repopulates exactly what it needs and precise eviction
		// would be bookkeeping for no benefit.
		if len(a.offsetSnapshots) >= maxCachedOffsetSnapshots {
			a.offsetSnapshots = nil
		}
		if a.offsetSnapshots == nil {
			a.offsetSnapshots = make(map[string]*offsetSnapshot, 8)
		}
		a.offsetSnapshots[key] = snapshot
		a.cacheMu.Unlock()
	}

	return snapshot, nil
}

// ConsumerLagRequest identifies the consumer group whose lag is to be measured.
type ConsumerLagRequest struct {
	// SubscriberID labels the measurement. It is used for the metric attribute and the log
	// line and takes no part in the computation, so a bare operational query may leave it
	// empty.
	SubscriberID string

	// GroupID is the consumer group to read committed offsets for. Required.
	GroupID string

	// Topics are the topics to measure. Normally the subscriber's authorised topics.
	Topics []string
}

// PartitionLag is the lag of one partition, with every input to the arithmetic
// retained.
type PartitionLag struct {
	// Topic and Partition identify the partition.
	Topic     string
	Partition int

	// CommittedOffset is the group's committed offset, or -1 when it has never committed
	// here. Committed says which of the two it is, so a caller never has to know that -1
	// is the sentinel.
	CommittedOffset int64
	Committed       bool

	// FirstOffset is the earliest offset still retained, which is above zero once
	// retention has deleted the head of the log. It is the baseline for a partition with
	// no commit.
	FirstOffset int64

	// EndOffset is the log end offset: the offset the next produced record will take.
	EndOffset int64

	// Lag is EndOffset minus the baseline, never negative.
	Lag int64

	// Unavailable is true when the broker could not report this partition's offsets, in
	// which case Lag is 0 and the partition contributes nothing. It is reported so a zero
	// caused by an unreadable partition is distinguishable from a zero caused by a
	// consumer that is keeping up.
	Unavailable bool
}

// TopicLag aggregates one topic's partitions.
type TopicLag struct {
	// Topic is the topic measured.
	Topic string

	// TotalLag is the sum of the partition lags, and the value published to the
	// consumer-lag gauge for this topic.
	TotalLag int64

	// Partitions carries the per-partition detail, in ascending partition order.
	Partitions []PartitionLag

	// PartitionsWithoutCommit counts partitions the group has never committed on. A number
	// equal to the partition count on a supposedly running consumer means the group is not
	// consuming this topic at all, which is a different fault from being behind.
	PartitionsWithoutCommit int

	// PartitionsUnavailable counts partitions whose offsets could not be read.
	PartitionsUnavailable int
}

// ConsumerLagReport is the outcome of one lag measurement.
type ConsumerLagReport struct {
	// SubscriberID and GroupID echo the request.
	SubscriberID string
	GroupID      string

	// TotalLag is the sum across every topic measured.
	TotalLag int64

	// Topics carries per-topic detail, in the order the request listed them.
	Topics []TopicLag

	// MissingTopics lists requested topics that do not exist on the broker. They
	// contribute no lag, and they are reported because a lag alert on a topic that
	// silently does not exist would read as permanently healthy.
	MissingTopics []string

	// MeasuredAt is when the measurement was taken. Committed offsets and end offsets are
	// read in two separate round trips, so under live traffic the figure is a snapshot of
	// two moments a few milliseconds apart, and a caller comparing it with anything else
	// needs to know when it was made.
	MeasuredAt time.Time
}

// LagByTopic reduces the report to the per-topic totals.
//
// Returns:
//   - map[string]int64: total lag keyed by topic, nil when nothing was measured.
func (r ConsumerLagReport) LagByTopic() map[string]int64 {
	if len(r.Topics) == 0 {
		return nil
	}

	lags := make(map[string]int64, len(r.Topics))
	for _, topic := range r.Topics {
		lags[topic.Topic] = topic.TotalLag
	}

	return lags
}

// LagSamples renders the report as inventory entries for the asynchronous consumer-lag
// gauges, one per topic measured.
//
// Returns:
//   - []metrics.ConsumerLagSample: one entry per measured topic, nil when nothing was
//     measured.
func (r ConsumerLagReport) LagSamples() []metrics.ConsumerLagSample {
	if len(r.Topics) == 0 {
		return nil
	}

	samples := make([]metrics.ConsumerLagSample, 0, len(r.Topics))
	for _, topicLag := range r.Topics {
		samples = append(samples, consumerLagSample(r.SubscriberID, r.GroupID, topicLag))
	}

	return samples
}

// ConsumerLag measures how far a consumer group trails the end of the log, in-process.
//
// Parameters:
//   - ctx context.Context: honoured before every round trip.
//   - req ConsumerLagRequest: the group and topics to measure.
//
// Returns:
//   - ConsumerLagReport: per-partition, per-topic and total lag.
//   - error: ErrKafkaAdminNotConfigured, a validation error for a missing group, or a
//     wrapped broker error.
func (a *KafkaAdminClient) ConsumerLag(ctx context.Context, req ConsumerLagRequest) (_ ConsumerLagReport, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "consumer_lag", hashedSubscriberSpanAttribute(req.SubscriberID))
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	report := ConsumerLagReport{
		SubscriberID: strings.TrimSpace(req.SubscriberID),
		GroupID:      strings.TrimSpace(req.GroupID),
		MeasuredAt:   time.Now().UTC(),
	}

	if err := a.ready(ctx); err != nil {
		return report, err
	}

	if report.GroupID == "" {
		return report, errors.New("kafka admin: a consumer group ID is required to measure consumer lag")
	}

	topics := normalizeTopicList(req.Topics)
	if len(topics) == 0 {
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash":  subscriberLogLabel(report.SubscriberID),
			"consumer_group_hash": consumerGroupLogLabel(report.GroupID),
		}).Debug("kafka admin: no topics to measure consumer lag for")

		return report, nil
	}

	// The partition layout and the end offsets come from ONE memoised snapshot, because
	// they are the two questions every subscriber in a sweep asks identically — only the
	// committed offsets below actually differ. Reading them together also keeps them
	// CONSISTENT with each other: a partition present in the layout but absent from the
	// bounds, or the reverse, would produce a lag computed from two different views of the
	// cluster.
	snapshot, err := a.partitionOffsetSnapshot(ctx, topics)
	partitions := snapshot.partitions
	report.MissingTopics = snapshot.missing
	if err != nil {
		return report, err
	}

	if len(report.MissingTopics) > 0 {
		logrus.WithFields(logrus.Fields{
			"subscriber_id_hash":  subscriberLogLabel(report.SubscriberID),
			"consumer_group_hash": consumerGroupLogLabel(report.GroupID),
			"topics":              report.MissingTopics,
		}).Warn("kafka admin: consumer lag was requested for topics that do not exist; they contribute no lag")
	}

	if len(partitions) == 0 {
		return report, nil
	}

	bounds := snapshot.bounds

	committed, err := a.committedOffsets(ctx, report.GroupID, partitions)
	if err != nil {
		return report, err
	}

	report.Topics = make([]TopicLag, 0, len(partitions))
	for _, topic := range topics {
		ids, exists := partitions[topic]
		if !exists {
			continue
		}

		topicLag := TopicLag{Topic: topic, Partitions: make([]PartitionLag, 0, len(ids))}
		for _, id := range ids {
			partitionLag := buildPartitionLag(
				topic, id,
				offsetBoundsFor(bounds, topic, id),
				committedOffsetFor(committed, topic, id),
			)

			topicLag.TotalLag += partitionLag.Lag
			// UNAVAILABLE FIRST: a partition nobody could read is not a partition without a
			// commit, and counting it in both would report the same fault twice under two
			// different diagnoses — one of which ("the group is not consuming this topic") sends
			// an operator to the consumer rather than to the broker.
			if partitionLag.Unavailable {
				topicLag.PartitionsUnavailable++
			} else if !partitionLag.Committed {
				topicLag.PartitionsWithoutCommit++
			}
			topicLag.Partitions = append(topicLag.Partitions, partitionLag)
		}

		report.TotalLag += topicLag.TotalLag
		report.Topics = append(report.Topics, topicLag)

		// A topic with unreadable partitions is reported LOUDLY, and this is the only place
		// the condition is stated per topic. The measurement continues — a partial total is
		// still the best diagnosis available and the caller receives it — but it must not be
		// mistaken for a complete one, so LagSamples marks it incomplete and the gauge the
		// alert reads does not receive it. See metrics.ConsumerLagUnmeasuredPartitions.
		if topicLag.PartitionsUnavailable > 0 {
			logrus.WithFields(logrus.Fields{
				"subscriber_id_hash":     subscriberLogLabel(report.SubscriberID),
				"consumer_group_hash":    consumerGroupLogLabel(report.GroupID),
				"topic":                  topic,
				"partitions_unavailable": topicLag.PartitionsUnavailable,
				"partitions":             len(topicLag.Partitions),
				"partial_lag":            topicLag.TotalLag,
			}).Warn(
				"kafka admin: some partitions could not be read, so this topic's lag is a LOWER BOUND and is " +
					"withheld from the consumer-lag gauge; the unmeasured-partition count is published instead " +
					"so the degraded measurement alerts rather than reading as healthy",
			)
		}
	}

	logrus.WithFields(logrus.Fields{
		"subscriber_id_hash":  subscriberLogLabel(report.SubscriberID),
		"consumer_group_hash": consumerGroupLogLabel(report.GroupID),
		"topics":              len(report.Topics),
		"total_lag":           report.TotalLag,
	}).Debug("kafka admin: consumer lag measured")

	return report, nil
}

// buildPartitionLag assembles one partition's entry from its offset bounds and
// committed offset.
func buildPartitionLag(
	topic string,
	partition int,
	bounds partitionOffsetBounds,
	committed committedOffset,
) PartitionLag {
	unavailable := bounds.unavailable || committed.unavailable

	lag := PartitionLag{
		Topic:           topic,
		Partition:       partition,
		CommittedOffset: committed.offset,
		// An unavailable reading is not evidence of an absent commit: the group may well have
		// committed here and the broker simply did not say. Reporting Committed false for it
		// would put the partition in PartitionsWithoutCommit, which is read as "this consumer
		// is not consuming the topic at all".
		Committed:   !committed.unavailable && committed.offset >= 0,
		FirstOffset: bounds.first,
		EndOffset:   bounds.end,
		Unavailable: unavailable,
	}

	if !unavailable {
		lag.Lag = lagForPartition(committed.offset, bounds.first, bounds.end)
	}

	return lag
}

// lagForPartition is the whole lag arithmetic, isolated so it can be tested
// exhaustively against a table with no broker in sight.
func lagForPartition(committedOffset, firstOffset, endOffset int64) int64 {
	if endOffset <= 0 {
		return 0
	}

	baseline := committedOffset
	if baseline < 0 {
		baseline = firstOffset
		if baseline < 0 {
			baseline = 0
		}
	}

	if baseline >= endOffset {
		return 0
	}

	return endOffset - baseline
}

// offsetBoundsFor reads one partition's offset window out of the ListOffsets result.
func offsetBoundsFor(
	bounds map[string]map[int]partitionOffsetBounds,
	topic string,
	partition int,
) partitionOffsetBounds {
	if perPartition, exists := bounds[topic]; exists {
		if bound, ok := perPartition[partition]; ok {
			return bound
		}
	}

	return partitionOffsetBounds{first: -1, end: -1, unavailable: true}
}

// committedOffsetFor reads one partition's committed-offset reading out of the fetch
// result.
func committedOffsetFor(
	committed map[string]map[int]committedOffset,
	topic string,
	partition int,
) committedOffset {
	if offsets, exists := committed[topic]; exists {
		if offset, ok := offsets[partition]; ok {
			return offset
		}
	}

	return committedOffset{offset: -1}
}

// consumerLagSample renders one topic's measurement as an inventory entry for the
// asynchronous consumer-lag gauges.
func consumerLagSample(subscriber, group string, topicLag TopicLag) metrics.ConsumerLagSample {
	return metrics.ConsumerLagSample{
		Subscriber:           subscriberLagLabel(subscriber),
		Group:                consumerGroupLagLabel(group),
		Topic:                topicLagLabel(topicLag.Topic),
		Lag:                  topicLag.TotalLag,
		LagComplete:          topicLag.PartitionsUnavailable == 0,
		UnmeasuredPartitions: topicLag.PartitionsUnavailable,
	}
}

// partitionOffsetBounds is one partition's offset window: the earliest offset still
// retained and the log end offset.
type partitionOffsetBounds struct {
	// first is the earliest retained offset, -1 when unknown.
	first int64

	// end is the log end offset, -1 when unknown.
	end int64

	// unavailable is true when the broker reported an error for this partition or gave
	// no usable end offset.
	unavailable bool
}

// offsetBounds reads the first and end offset of every given partition in ONE round
// trip.
func (a *KafkaAdminClient) offsetBounds(
	ctx context.Context,
	partitions map[string][]int,
) (map[string]map[int]partitionOffsetBounds, error) {
	request := &kafka.ListOffsetsRequest{
		Topics:         make(map[string][]kafka.OffsetRequest, len(partitions)),
		IsolationLevel: kafka.ReadUncommitted,
	}

	for topic, ids := range partitions {
		requests := make([]kafka.OffsetRequest, 0, len(ids)*2)
		for _, id := range ids {
			requests = append(requests, kafka.FirstOffsetOf(id), kafka.LastOffsetOf(id))
		}
		request.Topics[topic] = requests
	}

	response, err := a.client.ListOffsets(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("kafka admin: reading partition offsets: %w", err)
	}

	bounds := make(map[string]map[int]partitionOffsetBounds, len(response.Topics))
	for topic, offsets := range response.Topics {
		perPartition := make(map[int]partitionOffsetBounds, len(offsets))

		for _, offset := range offsets {
			bound := partitionOffsetBounds{first: offset.FirstOffset, end: offset.LastOffset}

			if offset.Error != nil || offset.LastOffset < 0 {
				bound.unavailable = true

				entry := logrus.WithFields(logrus.Fields{
					"topic":      topic,
					"partition":  offset.Partition,
					"end_offset": offset.LastOffset,
				})
				if offset.Error != nil {
					entry = withKafkaError(entry, "read_partition_end_offsets", offset.Error)
				}
				entry.Warn(
					"kafka admin: could not read offsets for this partition; it is excluded from the measurement " +
						"rather than counted as zero",
				)
			}

			perPartition[offset.Partition] = bound
		}

		bounds[topic] = perPartition
	}

	return bounds, nil
}

// committedOffsets reads a consumer group's committed offsets.
func (a *KafkaAdminClient) committedOffsets(
	ctx context.Context,
	group string,
	partitions map[string][]int,
) (map[string]map[int]committedOffset, error) {
	request := &kafka.OffsetFetchRequest{
		GroupID: group,
		Topics:  make(map[string][]int, len(partitions)),
	}

	for topic, ids := range partitions {
		// Copied so the request cannot alias, and later mutate, the caller's slices.
		copied := make([]int, len(ids))
		copy(copied, ids)
		request.Topics[topic] = copied
	}

	response, err := a.client.OffsetFetch(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("kafka admin: fetching committed offsets for consumer group %q: %w", group, err)
	}

	if response.Error != nil {
		if errors.Is(response.Error, kafka.GroupIdNotFound) {
			// DEBUG, not Info. This is the NORMAL state of every subscriber that has been
			// provisioned but has not connected yet, and the periodic collector re-measures
			// every registered subscriber on every tick — so at Info one un-started subscriber
			// produced a line per tick, indefinitely, for a condition that is not a fault. That
			// volume is not free: it is what trains an operator to filter the logger out, and it
			// takes the genuine warnings from this file with it.
			logrus.WithField("consumer_group_hash", consumerGroupLogLabel(group)).Debug(
				"kafka admin: consumer group does not exist yet, so it has committed nothing; " +
					"its lag is the whole retained log",
			)

			return nil, nil
		}

		return nil, fmt.Errorf(
			"kafka admin: fetching committed offsets for consumer group %q: %w", group, response.Error,
		)
	}

	committed := make(map[string]map[int]committedOffset, len(response.Topics))
	for topic, offsets := range response.Topics {
		perPartition := make(map[int]committedOffset, len(offsets))

		for _, offset := range offsets {
			if offset.Error != nil {
				// RECORDED AS UNAVAILABLE, not skipped.
				logrus.WithFields(logrus.Fields{
					"consumer_group_hash": consumerGroupLogLabel(group),
					"topic":               topic,
					"partition":           offset.Partition,
					"error_class":         kafkaErrorClassField("fetch_committed_offsets", offset.Error),
				}).Warn(
					"kafka admin: the broker could not report this partition's committed offset, so its " +
						"lag is UNKNOWN and is withheld rather than scored as uncommitted; the topic is " +
						"reported as incompletely measured",
				)

				perPartition[offset.Partition] = committedOffset{offset: -1, unavailable: true}

				continue
			}

			perPartition[offset.Partition] = committedOffset{offset: offset.CommittedOffset}
		}

		committed[topic] = perPartition
	}

	return committed, nil
}

// committedOffset is ONE partition's committed-offset reading, and it exists to keep
// two facts apart that a bare int64 conflated.
type committedOffset struct {
	// offset is the group's committed offset, or -1 when it has none. Meaningless when
	// unavailable is true.
	offset int64

	// unavailable is true when the broker returned a per-partition error for this
	// partition. The partition then contributes no lag and makes its topic's measurement
	// incomplete.
	unavailable bool
}

// PartitionOffsetSnapshot is one partition's offset window at a point in time.
type PartitionOffsetSnapshot struct {
	// Partition is the partition ID.
	Partition int

	// FirstOffset is the earliest offset still retained: the INCLUSIVE lower bound of the
	// window this partition can currently serve.
	FirstOffset int64

	// EndOffset is the log end offset, one past the last record written, which also equals
	// the total number of records ever produced to the partition.
	EndOffset int64

	// Unavailable is true when the broker could not report this partition, in which case
	// both offsets are meaningless: the partition contributes nothing to the sums and is
	// omitted from PartitionIntervals entirely, so the rows on it are classified as
	// unmeasured rather than misread as beyond the log end.
	Unavailable bool

	// WindowStartOffset is the earliest offset whose record was written at or after the
	// report's WindowStart: the left-hand side of the windowed record count. It is -1 when
	// the partition holds nothing that recent, and 0 with WindowUnreadable set when the
	// window could not be resolved for this partition.
	WindowStartOffset int64

	// WindowUnreadable is true when the window-start offset could not be resolved, so this
	// partition contributes nothing to the windowed count and the count is incomplete.
	WindowUnreadable bool
}

// TopicOffsetSnapshot aggregates one topic's partitions.
type TopicOffsetSnapshot struct {
	// Topic is the topic measured.
	Topic string

	// Partitions carries the per-partition detail, in ascending partition order.
	Partitions []PartitionOffsetSnapshot

	// EndOffsetSum is the sum of the partitions' end offsets: every record ever published
	// to this topic BY ANY PRODUCER, whether or not it is still retained.
	EndOffsetSum int64

	// RetainedCount is the sum of end minus first across partitions: the records still on
	// the log. Like EndOffsetSum it is context rather than proof, and it is reported so
	// that the share of a topic's traffic the verdict accounts for is readable beside it.
	RetainedCount int64

	// PartitionsUnavailable counts partitions excluded from the sums.
	PartitionsUnavailable int

	// WindowRecordCount is how many records this topic accepted inside the report's
	// window: the sum over partitions of end offset minus window-start offset. It is the
	// figure the WINDOWED reconciliation compares the outbox against, and it is zero when
	// the report carries no window.
	WindowRecordCount int64

	// WindowTruncated is true when retention has removed records that were written inside
	// the window, so this topic's window count is a LOWER bound. It is detected rather
	// than assumed: a partition whose window-start offset equals its first retained
	// offset, on a partition that has already had records deleted, cannot rule out that
	// earlier records inside the window are gone.
	WindowTruncated bool

	// WindowPartitionsUnreadable counts partitions whose window-start offset could not be
	// resolved, so their records are missing from WindowRecordCount.
	WindowPartitionsUnreadable int
}

// TopicOffsetReport is the broker-side half of the zero-loss reconciliation.
type TopicOffsetReport struct {
	// Topics carries per-topic detail, in the order the request listed them, or the
	// canonical inventory order when the request named no topics.
	Topics []TopicOffsetSnapshot

	// EndOffsetSum is the total number of RECORDS WRITTEN across every topic measured, and
	// RetainedCount how many of those the broker still holds.
	EndOffsetSum  int64
	RetainedCount int64

	// MissingTopics lists requested topics that do not exist on the broker.
	MissingTopics []string

	// PartitionsUnavailable counts partitions excluded from the totals across all
	// topics.
	PartitionsUnavailable int

	// MeasuredAt is when the snapshot was taken.
	MeasuredAt time.Time

	// WindowStart is the instant the windowed figures below are measured from, and the
	// zero value means the report carries no window.
	WindowStart time.Time

	// WindowRecordCount is how many records the broker accepted across every measured topic
	// inside the window. This is the figure a windowed reconciliation compares against.
	WindowRecordCount int64

	// WindowTruncated is true when retention has removed records written inside the window
	// on at least one partition, which makes WindowRecordCount a lower bound and the
	// verdict inconclusive: a short broker retention against a longer reconciliation
	// window is exactly the configuration that would otherwise report loss that never
	// happened.
	WindowTruncated bool

	// WindowPartitionsUnreadable counts partitions whose window-start offset could not be
	// resolved across all topics.
	WindowPartitionsUnreadable int
}

// EndOffsetsByTopic reduces the report to one end-offset sum per topic.
//
// Returns:
//   - map[string]int64: end-offset sum keyed by topic, nil when nothing was measured.
func (r TopicOffsetReport) EndOffsetsByTopic() map[string]int64 {
	if len(r.Topics) == 0 {
		return nil
	}

	offsets := make(map[string]int64, len(r.Topics))
	for _, topic := range r.Topics {
		offsets[topic.Topic] = topic.EndOffsetSum
	}

	return offsets
}

// Lookup finds one topic's snapshot.
//
// Parameters:
//   - topic string: a fully-qualified topic name.
//
// Returns:
//   - TopicOffsetSnapshot: the entry, or the zero value when absent.
//   - bool: whether the topic appears in the report.
func (r TopicOffsetReport) Lookup(topic string) (TopicOffsetSnapshot, bool) {
	for _, snapshot := range r.Topics {
		if snapshot.Topic == topic {
			return snapshot, true
		}
	}

	return TopicOffsetSnapshot{}, false
}

// TopicEndOffsets measures the PER-PARTITION OFFSET WINDOWS the daily zero-loss
// reconciliation classifies the outbox against.
//
// Parameters:
//   - ctx context.Context: honoured before every round trip.
//   - topics ...string: optional topic names. Blanks and duplicates are dropped; an
//     entirely empty list selects the full inventory.
//
// Returns:
//   - TopicOffsetReport: the per-partition windows, the sums, and the caveats
//     (MissingTopics, PartitionsUnavailable) that say what could not be measured.
//   - error: ErrKafkaAdminNotConfigured, or a wrapped broker error.
func (a *KafkaAdminClient) TopicEndOffsets(
	ctx context.Context,
	since time.Time,
	topics ...string,
) (_ TopicOffsetReport, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "topic_end_offsets")
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	report := TopicOffsetReport{MeasuredAt: time.Now().UTC()}
	if err := a.ready(ctx); err != nil {
		return report, err
	}

	requested := normalizeTopicList(topics)
	if len(requested) == 0 {
		// The inventory, from its single source of truth, so the reconciliation covers
		// exactly the topics the pipeline provisions and publishes to — across every owned
		// prefix, because the outbox rows this is reconciled against may name a namespace the
		// deployment has since renamed away from. Omitting a historical topic would leave its
		// dispatched rows counted with no offsets to match them, which reads as loss that did
		// not happen.
		requested = AllOwnedTopicsAcrossPrefixes()
	}

	partitions, err := a.topicPartitions(ctx, requested)
	report.MissingTopics = missingTopics(requested, partitions)
	if err != nil {
		return report, err
	}

	if len(partitions) == 0 {
		return report, nil
	}

	bounds, err := a.offsetBounds(ctx, partitions)
	if err != nil {
		return report, err
	}

	// The window's left-hand side, read only when one was asked for. A failure here does NOT
	// void the report: the cumulative figures are still valid and are what the diagnostic
	// reading wants, so the window is simply left unset and ReconcileAgainstOutbox declines to
	// call the comparison conclusive — which is the honest outcome of a window that could not
	// be measured.
	var windowStarts map[string]map[int]int64
	if !since.IsZero() {
		report.WindowStart = since.UTC()

		windowStarts, err = a.windowStartOffsets(ctx, partitions, since)
		if err != nil {
			kafkaErrorEntry("topic_window_start_offsets", err).Error(
				"kafka admin: the window-start offsets could not be read, so the reconciliation has no " +
					"bounded broker-side population and its verdict will be reported inconclusive",
			)

			report.WindowStart = time.Time{}
		}
	}

	report.Topics = make([]TopicOffsetSnapshot, 0, len(partitions))
	for _, topic := range requested {
		ids, exists := partitions[topic]
		if !exists {
			continue
		}

		snapshot := TopicOffsetSnapshot{Topic: topic, Partitions: make([]PartitionOffsetSnapshot, 0, len(ids))}
		for _, id := range ids {
			bound := offsetBoundsFor(bounds, topic, id)

			partitionSnapshot := PartitionOffsetSnapshot{
				Partition:         id,
				FirstOffset:       bound.first,
				EndOffset:         bound.end,
				WindowStartOffset: -1,
				Unavailable:       bound.unavailable,
			}

			if bound.unavailable {
				snapshot.PartitionsUnavailable++
			} else {
				snapshot.EndOffsetSum += bound.end
				snapshot.RetainedCount += retainedRecords(bound.first, bound.end)

				if !report.WindowStart.IsZero() {
					applyWindowToPartition(&partitionSnapshot, &snapshot, windowStarts, topic, id, bound)
				}
			}

			snapshot.Partitions = append(snapshot.Partitions, partitionSnapshot)
		}

		report.EndOffsetSum += snapshot.EndOffsetSum
		report.RetainedCount += snapshot.RetainedCount
		report.PartitionsUnavailable += snapshot.PartitionsUnavailable
		report.WindowRecordCount += snapshot.WindowRecordCount
		report.WindowPartitionsUnreadable += snapshot.WindowPartitionsUnreadable
		report.WindowTruncated = report.WindowTruncated || snapshot.WindowTruncated
		report.Topics = append(report.Topics, snapshot)
	}

	report.MeasuredAt = time.Now().UTC()

	logrus.WithFields(logrus.Fields{
		"topics":                 len(report.Topics),
		"end_offset_sum":         report.EndOffsetSum,
		"retained":               report.RetainedCount,
		"window_start":           report.WindowStart.Format(time.RFC3339),
		"window_records":         report.WindowRecordCount,
		"window_truncated":       report.WindowTruncated,
		"missing_topics":         len(report.MissingTopics),
		"partitions_unavailable": report.PartitionsUnavailable,
	}).Debug("kafka admin: topic end offsets read")

	return report, nil
}

// retainedRecords counts the records still on a partition's log.
func retainedRecords(firstOffset, endOffset int64) int64 {
	if endOffset <= 0 {
		return 0
	}

	first := firstOffset
	if first < 0 {
		first = 0
	}

	if first >= endOffset {
		return 0
	}

	return endOffset - first
}

// PartitionIntervals flattens the report into the measured windows the outbox audit is
// classified against.
//
// Returns:
//   - []model.PartitionOffsetInterval: one window per measured, available partition, in
//     report order.
func (r TopicOffsetReport) PartitionIntervals() []model.PartitionOffsetInterval {
	intervals := make([]model.PartitionOffsetInterval, 0, len(r.Topics))
	for _, topic := range r.Topics {
		for _, partition := range topic.Partitions {
			if partition.Unavailable {
				continue
			}

			intervals = append(intervals, model.PartitionOffsetInterval{
				Topic:       topic.Topic,
				Partition:   partition.Partition,
				FirstOffset: partition.FirstOffset,
				EndOffset:   partition.EndOffset,
			})
		}
	}

	return intervals
}

// windowStartOffsets resolves, per partition, the earliest offset whose record was
// written at or after the given instant.
func (a *KafkaAdminClient) windowStartOffsets(
	ctx context.Context,
	partitions map[string][]int,
	since time.Time,
) (map[string]map[int]int64, error) {
	request := &kafka.ListOffsetsRequest{
		Topics:         make(map[string][]kafka.OffsetRequest, len(partitions)),
		IsolationLevel: kafka.ReadUncommitted,
	}

	for topic, ids := range partitions {
		requests := make([]kafka.OffsetRequest, 0, len(ids))
		for _, id := range ids {
			requests = append(requests, kafka.TimeOffsetOf(id, since))
		}
		request.Topics[topic] = requests
	}

	response, err := a.client.ListOffsets(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("kafka admin: reading window-start offsets: %w", err)
	}

	starts := make(map[string]map[int]int64, len(response.Topics))
	for topic, offsets := range response.Topics {
		perPartition := make(map[int]int64, len(offsets))

		for _, offset := range offsets {
			if offset.Error != nil {
				logrus.WithFields(logrus.Fields{
					"topic":     topic,
					"partition": offset.Partition,
					"error":     sanitizeLogValue(offset.Error.Error(), maxLoggedErrorLength),
				}).Warn(
					"kafka admin: could not resolve the window-start offset for this partition; it is " +
						"excluded from the windowed reconciliation rather than counted as empty",
				)

				continue
			}

			// A timestamp request's answer arrives in the Offsets map rather than in FirstOffset
			// or LastOffset, which kafka-go reserves for the two sentinel timestamps. The map
			// holds one entry per timestamp asked about, and exactly one was asked about here;
			// the broker's "nothing that recent" answer is the offset -1, which is carried
			// through unchanged for the caller to interpret.
			resolved := int64(-1)
			for candidate := range offset.Offsets {
				resolved = candidate

				break
			}

			perPartition[offset.Partition] = resolved
		}

		starts[topic] = perPartition
	}

	return starts, nil
}

// windowStartOffsetFor reads one partition's window-start offset out of the result.
func windowStartOffsetFor(starts map[string]map[int]int64, topic string, partition int) (int64, bool) {
	if perPartition, exists := starts[topic]; exists {
		if offset, ok := perPartition[partition]; ok {
			return offset, true
		}
	}

	return -1, false
}

// applyWindowToPartition folds one partition's windowed record count into its snapshot
// and its topic's totals.
func applyWindowToPartition(
	partitionSnapshot *PartitionOffsetSnapshot,
	snapshot *TopicOffsetSnapshot,
	windowStarts map[string]map[int]int64,
	topic string,
	partition int,
	bound partitionOffsetBounds,
) {
	start, readable := windowStartOffsetFor(windowStarts, topic, partition)
	if !readable {
		partitionSnapshot.WindowUnreadable = true
		snapshot.WindowPartitionsUnreadable++

		return
	}

	partitionSnapshot.WindowStartOffset = start
	if start < 0 {
		// Every record predates the window: a real zero, not an absence.
		return
	}

	if start <= bound.first && bound.first > 0 {
		// The oldest record the partition still holds is already inside the window, so
		// retention may have removed earlier records that were inside it too.
		snapshot.WindowTruncated = true
	}

	if written := bound.end - start; written > 0 {
		snapshot.WindowRecordCount += written
	}
}
