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
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
)

// TopicAssurance is what happened to one topic during EnsureTopics.
type TopicAssurance struct {
	// Topic is the fully-qualified topic name, as resolved by event_topics.go.
	Topic string

	// Created is true when this run created the topic.
	Created bool

	// PartitionsBefore is the partition count found before this run, and 0 for a topic
	// this run created.
	PartitionsBefore int

	// PartitionsAfter is the partition count in force after this run.
	PartitionsAfter int

	// PartitionsAdded is true when this run grew an existing topic.
	PartitionsAdded bool

	// ShrinkRefused is true when the topic has MORE partitions than configured and was
	// deliberately left alone. See EnsureTopics for why shrinking is never attempted.
	ShrinkRefused bool

	// GrowthRefused is true when the topic has FEWER partitions than configured, already
	// holds records, and was therefore not grown.
	GrowthRefused bool

	// ReplicationFactor is the topic's OBSERVED minimum replica count across its
	// partitions, or the factor it was created with for a topic this run created. Zero
	// means the topic was not visible in metadata yet.
	ReplicationFactor int

	// ReplicationInadequate is true when the observed replica count is below the
	// configured KAFKA_REPLICATION_FACTOR.
	ReplicationInadequate bool
}

// TopicAssuranceReport is the outcome of one EnsureTopics call across every topic Blnk
// owns.
type TopicAssuranceReport struct {
	// Topics carries one entry per topic, in the canonical order AllTopicsWithDeadLetters
	// returns: every category topic, then each one's dead-letter siblings. The stable
	// order is what lets the report be diffed against the provisioning script line for
	// line.
	Topics []TopicAssurance

	// GrowthRefusedCount is how many topics needed partitions and were not grown because
	// they already hold records. A non-zero value is a geometry defect that needs a
	// planned migration, and EnsureTopics returns ErrPartitionGrowthRefused alongside it.
	GrowthRefusedCount int

	// Partitions is the partition count applied, after the MinTopicPartitions floor.
	Partitions int

	// ReplicationFactor is the factor applied to newly created topics, straight from
	// configuration.
	ReplicationFactor int

	// CreatedCount, GrownCount, UnchangedCount and ShrinkRefusedCount summarise Topics.
	CreatedCount       int
	GrownCount         int
	UnchangedCount     int
	ShrinkRefusedCount int

	// CompletedAt is when the assurance finished.
	CompletedAt time.Time
}

// Lookup finds the assurance for one topic.
//
// Parameters:
//   - topic string: a fully-qualified topic name.
//
// Returns:
//   - TopicAssurance: the entry, or the zero value when absent.
//   - bool: whether the topic appears in the report.
func (r TopicAssuranceReport) Lookup(topic string) (TopicAssurance, bool) {
	for _, assurance := range r.Topics {
		if assurance.Topic == topic {
			return assurance, true
		}
	}

	return TopicAssurance{}, false
}

// TopicNames returns the topic names covered by the report, in canonical order.
//
// Returns:
//   - []string: a fresh slice, nil when the report is empty.
func (r TopicAssuranceReport) TopicNames() []string {
	if len(r.Topics) == 0 {
		return nil
	}

	names := make([]string, 0, len(r.Topics))
	for _, assurance := range r.Topics {
		names = append(names, assurance.Topic)
	}

	return names
}

// maxTopicPartitions bounds the configured partition count.
const maxTopicPartitions = 10000

// topicCreationOutcome records which topics a create pass actually created and which
// ones turned out to exist already.
type topicCreationOutcome struct {
	// created holds the topics this process created.
	created map[string]bool

	// raced holds topics that the metadata probe reported as absent but that the broker
	// answered TOPIC_ALREADY_EXISTS for. Another provisioner — a second server instance,
	// or scripts/kafka-provision.sh — created them in between. Their partition count is
	// unknown at that point and has to be re-probed before the grow pass can decide
	// anything.
	raced []string
}

// EnsureTopics creates every topic Blnk publishes to and grows any that exist with too
// few partitions.
//
// Parameters:
//   - ctx context.Context: cancelled or expired before any round trip is attempted.
//
// Returns:
//   - TopicAssuranceReport: populated even when an error is returned, so a caller can
//     see how far the assurance got.
//   - error: ErrKafkaAdminNotConfigured when no broker is configured, an actionable
//     error when the replication factor is unconfigured, ErrPartitionGrowthRefused when
//     a non-empty topic needs growing, ErrReplicationFactorInadequate when an existing
//     topic is under-replicated, or a wrapped broker error.
func (a *KafkaAdminClient) EnsureTopics(ctx context.Context) (_ TopicAssuranceReport, err error) {
	ctx, span := startKafkaAdminSpan(ctx, "ensure_topics")
	defer span.End()
	defer func() { failKafkaAdminSpan(span, err) }()

	report := TopicAssuranceReport{CompletedAt: time.Now().UTC()}
	if err := a.ready(ctx); err != nil {
		return report, err
	}

	report.Partitions = a.partitions
	report.ReplicationFactor = a.replicationFactor

	if a.replicationFactor < 1 {
		return report, errors.New(
			"kafka admin: KAFKA_REPLICATION_FACTOR is not configured, so topics cannot be created; " +
				"set it to 3 on a replicated production cluster, or to 1 on a single-broker stack, " +
				"which cannot satisfy a higher factor",
		)
	}

	// The inventory comes from event_topics.go, never from literals here, so this
	// operation and scripts/kafka-provision.sh provision exactly the same topics under
	// whatever KAFKA_TOPIC_PREFIX is configured.
	desired := AllOwnedTopicsAcrossPrefixes()

	partitionsBefore, err := a.partitionCounts(ctx, desired)
	if err != nil {
		return report, err
	}

	outcome, err := a.createMissingTopics(ctx, desired, partitionsBefore)
	if err != nil {
		return report, err
	}

	if len(outcome.raced) > 0 {
		racedCounts, probeErr := a.partitionCounts(ctx, outcome.raced)
		if probeErr != nil {
			return report, probeErr
		}
		for topic, count := range racedCounts {
			partitionsBefore[topic] = count
		}
	}

	// Re-read as metadata rather than counts, because the replica sets are needed too and a
	// second read would observe a different moment from the one the counts came from.
	observed, err := a.topicMetadata(ctx, desired)
	if err != nil {
		return report, err
	}

	plan := a.planAssurance(desired, partitionsBefore, outcome)
	report.Topics = plan.assurances
	report.CreatedCount = plan.created
	report.UnchangedCount = plan.unchanged
	report.ShrinkRefusedCount = plan.shrinkRefused

	replicationErr := a.verifyReplication(report.Topics, observed)

	var growthErr error
	if len(plan.grow) > 0 {
		growable, refused, inspectErr := a.partitionGrowthDecision(ctx, plan.grow, observed)
		if inspectErr != nil {
			return report, inspectErr
		}

		if len(refused) > 0 {
			a.markGrowthRefused(report.Topics, refused)
			report.GrowthRefusedCount = len(refused)
			growthErr = growthRefusedError(refused, a.partitions)
		}

		if len(growable) > 0 {
			// The plan describes the topics as they are, so a failure here returns a report that
			// still says "one partition", not one that claims a growth that did not happen. The
			// entries are only updated once the broker has confirmed it.
			if growErr := a.growTopics(ctx, growable); growErr != nil {
				return report, growErr
			}

			a.markPartitionsGrown(report.Topics, growable)
			report.GrownCount = len(growable)
		}
	}

	report.CompletedAt = time.Now().UTC()

	// Any cached partition layout is now KNOWN to be wrong rather than merely old: a topic
	// was created or repartitioned. Dropping the snapshot here — rather than waiting for
	// the TTL — is what stops a lag measurement taken straight after provisioning from
	// reporting against a layout that no longer exists.
	if report.CreatedCount > 0 || report.GrownCount > 0 {
		a.InvalidateOffsetSnapshot()
	}

	logrus.WithFields(logrus.Fields{
		"topics":             len(report.Topics),
		"created":            report.CreatedCount,
		"grown":              report.GrownCount,
		"unchanged":          report.UnchangedCount,
		"shrink_refused":     report.ShrinkRefusedCount,
		"growth_refused":     report.GrowthRefusedCount,
		"partitions":         report.Partitions,
		"replication_factor": report.ReplicationFactor,
	}).Info("kafka admin: event topics assured")

	// Joined rather than short-circuited: an operator fixing a geometry problem needs to see
	// every geometry problem, not the first one in an arbitrary order.
	return report, errors.Join(replicationErr, growthErr)
}

// ErrPartitionGrowthRefused reports that a topic needs more partitions and already holds
// records, so growing it would re-map keys and split aggregate histories.
var ErrPartitionGrowthRefused = errors.New(
	"kafka admin: refusing to add partitions to a topic that already holds records",
)

// ErrReplicationFactorInadequate reports that an existing topic has fewer replicas than the
// configured replication factor.
var ErrReplicationFactorInadequate = errors.New(
	"kafka admin: an existing topic has fewer replicas than KAFKA_REPLICATION_FACTOR requires",
)

// verifyReplication records the observed replica count on each assurance entry and
// reports every topic that falls short of the configured factor.
func (a *KafkaAdminClient) verifyReplication(
	assurances []TopicAssurance,
	observed map[string]observedTopicMetadata,
) error {
	short := make([]string, 0, len(assurances))

	for i := range assurances {
		metadata, exists := observed[assurances[i].Topic]
		if !exists || metadata.minReplicas <= 0 {
			// The topic is not visible yet — created moments ago, or its leader is still being
			// assigned. Its factor was set by whoever created it and cannot be read now; the
			// next assurance pass reads it.
			continue
		}

		assurances[i].ReplicationFactor = metadata.minReplicas

		if metadata.minReplicas < a.replicationFactor {
			assurances[i].ReplicationInadequate = true
			short = append(short, fmt.Sprintf("%s (%d)", assurances[i].Topic, metadata.minReplicas))

			logrus.WithFields(logrus.Fields{
				"topic":             assurances[i].Topic,
				"observed_replicas": metadata.minReplicas,
				"configured_factor": a.replicationFactor,
				"consequence":       "events on this topic are lost if that broker is lost",
				"remedy":            "reassign partitions with kafka-reassign-partitions",
			}).Error(
				"kafka admin: existing topic is under-replicated relative to KAFKA_REPLICATION_FACTOR; " +
					"a replication factor cannot be raised by creating a topic that already exists",
			)
		}
	}

	if len(short) == 0 {
		return nil
	}

	return fmt.Errorf(
		"%w: %s each have fewer replicas than the configured factor %d. Raising the factor of an "+
			"existing topic requires a partition reassignment (kafka-reassign-partitions); it cannot be "+
			"done by re-running topic assurance",
		ErrReplicationFactorInadequate, strings.Join(short, ", "), a.replicationFactor,
	)
}

// partitionGrowthDecision splits the topics that need growing into those that may be
// grown and those that must not be.
func (a *KafkaAdminClient) partitionGrowthDecision(
	ctx context.Context,
	candidates []string,
	observed map[string]observedTopicMetadata,
) (growable, refused []string, err error) {
	partitions := make(map[string][]int, len(candidates))
	for _, topic := range candidates {
		if metadata, exists := observed[topic]; exists && len(metadata.partitionIDs) > 0 {
			partitions[topic] = metadata.partitionIDs
		}
	}

	holding, err := a.topicsHoldingRecords(ctx, partitions)
	if err != nil {
		return nil, nil, err
	}

	growable = make([]string, 0, len(candidates))
	refused = make([]string, 0, len(candidates))

	for _, topic := range candidates {
		records, occupied := holding[topic]
		switch {
		case !occupied:
			growable = append(growable, topic)

		case a.allowPartitionGrowth:
			logrus.WithFields(logrus.Fields{
				"topic":      topic,
				"records":    records,
				"partitions": a.partitions,
			}).Warn(
				"kafka admin: growing a topic that holds records because KAFKA_ALLOW_PARTITION_GROWTH is " +
					"set. Keys already written will re-map to different partitions, so the per-aggregate " +
					"ordering of existing events is not preserved",
			)

			growable = append(growable, topic)

		default:
			refused = append(refused, topic)
		}
	}

	return growable, refused, nil
}

// markGrowthRefused records a refusal on the report entries.
func (a *KafkaAdminClient) markGrowthRefused(assurances []TopicAssurance, refused []string) {
	names := make(map[string]struct{}, len(refused))
	for _, topic := range refused {
		names[topic] = struct{}{}
	}

	for i := range assurances {
		if _, ok := names[assurances[i].Topic]; ok {
			assurances[i].GrowthRefused = true
		}
	}
}

// growthRefusedError builds the actionable message for a refused growth.
func growthRefusedError(refused []string, configured int) error {
	return fmt.Errorf(
		"%w: %s hold records and have fewer than the configured %d partitions. Adding partitions would "+
			"re-map keys and split existing aggregate histories across partitions, breaking per-aggregate "+
			"ordering irreversibly. Provision a correctly-shaped topic and migrate consumers to it, or set "+
			"KAFKA_ALLOW_PARTITION_GROWTH once that migration is planned",
		ErrPartitionGrowthRefused, strings.Join(refused, ", "), configured,
	)
}

// topicAssurancePlan is what planAssurance decided: the per-topic report entries as the
// topics stand right now, plus the list of topics that still need growing.
type topicAssurancePlan struct {
	assurances    []TopicAssurance
	grow          []string
	created       int
	unchanged     int
	shrinkRefused int
}

// planAssurance decides, per topic, what state it is in and whether it needs growing.
func (a *KafkaAdminClient) planAssurance(
	desired []string,
	partitionsBefore map[string]int,
	outcome topicCreationOutcome,
) topicAssurancePlan {
	plan := topicAssurancePlan{assurances: make([]TopicAssurance, 0, len(desired))}

	for _, topic := range desired {
		assurance := TopicAssurance{Topic: topic, PartitionsAfter: a.partitions}

		existing, exists := partitionsBefore[topic]
		switch {
		case !exists:
			// Created by this run, or by a concurrent provisioner whose partitions the re-probe
			// could not yet see. Either way the topic now exists and its geometry was decided by
			// whoever created it.
			assurance.Created = outcome.created[topic]
			if assurance.Created {
				assurance.ReplicationFactor = a.replicationFactor
				plan.created++
			} else {
				plan.unchanged++
			}

		case existing < a.partitions:
			// Reported as it stands. markPartitionsGrown updates it once the broker has
			// actually added the partitions.
			assurance.PartitionsBefore = existing
			assurance.PartitionsAfter = existing
			plan.grow = append(plan.grow, topic)

		case existing > a.partitions:
			assurance.PartitionsBefore = existing
			assurance.PartitionsAfter = existing
			assurance.ShrinkRefused = true
			plan.shrinkRefused++

			logrus.WithFields(logrus.Fields{
				"topic":         topic,
				"partitions":    existing,
				"configured":    a.partitions,
				"action":        "left unchanged",
				"why_no_shrink": "kafka cannot reduce a partition count, and doing so would move keys between partitions",
			}).Warn(
				"kafka admin: topic has more partitions than configured; it is being left alone. Either raise " +
					"KAFKA_MIN_PARTITIONS to match the topic, or accept the wider layout — reducing partitions " +
					"is impossible in Kafka and would break per-aggregate ordering if it were not",
			)

		default:
			assurance.PartitionsBefore = existing
			plan.unchanged++
		}

		plan.assurances = append(plan.assurances, assurance)
	}

	return plan
}

// markPartitionsGrown records a confirmed growth on the report entries.
func (a *KafkaAdminClient) markPartitionsGrown(assurances []TopicAssurance, grown []string) {
	confirmed := make(map[string]struct{}, len(grown))
	for _, topic := range grown {
		confirmed[topic] = struct{}{}
	}

	for i := range assurances {
		if _, ok := confirmed[assurances[i].Topic]; !ok {
			continue
		}
		assurances[i].PartitionsAdded = true
		assurances[i].PartitionsAfter = a.partitions
	}
}

// createMissingTopics creates the topics the probe did not find, with the configured
// geometry.
func (a *KafkaAdminClient) createMissingTopics(
	ctx context.Context,
	desired []string,
	partitionsBefore map[string]int,
) (topicCreationOutcome, error) {
	outcome := topicCreationOutcome{created: make(map[string]bool, len(desired))}

	configs := make([]kafka.TopicConfig, 0, len(desired))
	for _, topic := range desired {
		if _, exists := partitionsBefore[topic]; exists {
			continue
		}
		configs = append(configs, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     a.partitions,
			ReplicationFactor: a.replicationFactor,
		})
	}

	if len(configs) == 0 {
		return outcome, nil
	}

	response, err := a.client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: configs})
	if err != nil {
		return outcome, fmt.Errorf(
			"kafka admin: creating %d event topic(s) with %d partitions and replication factor %d: %w",
			len(configs), a.partitions, a.replicationFactor, err,
		)
	}

	for topic, topicErr := range response.Errors {
		switch {
		case topicErr == nil:
			outcome.created[topic] = true

			logrus.WithFields(logrus.Fields{
				"topic":              topic,
				"partitions":         a.partitions,
				"replication_factor": a.replicationFactor,
			}).Info("kafka admin: event topic created")

		case errors.Is(topicErr, kafka.TopicAlreadyExists):
			// Not an error. Another provisioner won the race; the grow pass will bring
			// the topic up to the configured partition count if it needs it.
			outcome.raced = append(outcome.raced, topic)

			logrus.WithField("topic", topic).Info(
				"kafka admin: event topic already exists; leaving it in place and checking its partition count",
			)

		case errors.Is(topicErr, kafka.InvalidReplicationFactor):
			return outcome, fmt.Errorf(
				"kafka admin: cannot create topic %q with replication factor %d — the cluster has fewer brokers "+
					"than that. Set KAFKA_REPLICATION_FACTOR to at most the broker count (1 for the "+
					"single-broker local stack): %w",
				topic, a.replicationFactor, topicErr,
			)

		default:
			return outcome, fmt.Errorf("kafka admin: creating topic %q: %w", topic, topicErr)
		}
	}

	// Sorted so that the re-probe request, and any log line derived from it, is
	// deterministic; response.Errors is a map and iterates in random order.
	sort.Strings(outcome.raced)

	return outcome, nil
}

// growTopics raises the partition count of existing topics to the configured value.
func (a *KafkaAdminClient) growTopics(ctx context.Context, topics []string) error {
	configs := make([]kafka.TopicPartitionsConfig, 0, len(topics))
	for _, topic := range topics {
		configs = append(configs, kafka.TopicPartitionsConfig{
			Name: topic,
			//nolint:gosec // resolveTopicPartitions bounds the count well inside int32.
			Count: int32(a.partitions),
		})
	}

	response, err := a.client.CreatePartitions(ctx, &kafka.CreatePartitionsRequest{Topics: configs})
	if err != nil {
		return fmt.Errorf("kafka admin: growing %d topic(s) to %d partitions: %w", len(configs), a.partitions, err)
	}

	for topic, topicErr := range response.Errors {
		switch {
		case topicErr == nil:
			logrus.WithFields(logrus.Fields{
				"topic":      topic,
				"partitions": a.partitions,
			}).Info("kafka admin: event topic partitions increased")

		case errors.Is(topicErr, kafka.InvalidPartitionNumber):
			logrus.WithFields(logrus.Fields{
				"topic":      topic,
				"partitions": a.partitions,
			}).Info(
				"kafka admin: topic already has at least the configured partition count; nothing to grow",
			)

		default:
			return fmt.Errorf(
				"kafka admin: growing topic %q to %d partitions: %w", topic, a.partitions, topicErr,
			)
		}
	}

	return nil
}

// topicPartitions probes which of the given topics exist and which partition IDs each
// has.
func (a *KafkaAdminClient) topicPartitions(ctx context.Context, topics []string) (map[string][]int, error) {
	metadata, err := a.topicMetadata(ctx, topics)
	if err != nil {
		return nil, err
	}

	partitions := make(map[string][]int, len(metadata))
	for topic := range metadata {
		partitions[topic] = metadata[topic].partitionIDs
	}

	return partitions, nil
}

// observedTopicMetadata is what the broker says about one existing topic.
type observedTopicMetadata struct {
	// partitionIDs are the topic's partition IDs, ascending.
	partitionIDs []int

	// minReplicas is the SMALLEST replica-set size across the topic's partitions.
	minReplicas int
}

// topicMetadata probes which of the given topics exist, which partition IDs each has,
// and how many replicas the least-replicated partition of each has.
func (a *KafkaAdminClient) topicMetadata(
	ctx context.Context,
	topics []string,
) (map[string]observedTopicMetadata, error) {
	response, err := a.client.Metadata(ctx, &kafka.MetadataRequest{Topics: topics})
	if err != nil {
		return nil, fmt.Errorf("kafka admin: reading topic metadata: %w", err)
	}

	partitions := make(map[string]observedTopicMetadata, len(response.Topics))
	for _, topic := range response.Topics {
		if len(topic.Partitions) > 0 {
			ids := make([]int, 0, len(topic.Partitions))
			minReplicas := -1
			for _, partition := range topic.Partitions {
				ids = append(ids, partition.ID)
				if replicas := len(partition.Replicas); minReplicas < 0 || replicas < minReplicas {
					minReplicas = replicas
				}
			}
			sort.Ints(ids)
			partitions[topic.Name] = observedTopicMetadata{partitionIDs: ids, minReplicas: minReplicas}

			continue
		}

		switch {
		case topic.Error == nil,
			errors.Is(topic.Error, kafka.UnknownTopicOrPartition),
			errors.Is(topic.Error, kafka.LeaderNotAvailable):
			// Absent, or newly created and not yet assigned a leader. Both are treated as "not
			// there yet"; the create pass turns the first into an existing topic and the second
			// into a harmless TOPIC_ALREADY_EXISTS.
			logrus.WithField("topic", topic.Name).Debug("kafka admin: topic not present yet")

		default:
			return nil, fmt.Errorf("kafka admin: reading metadata for topic %q: %w", topic.Name, topic.Error)
		}
	}

	return partitions, nil
}

// partitionCounts is topicMetadata reduced to a count per topic.
func (a *KafkaAdminClient) partitionCounts(ctx context.Context, topics []string) (map[string]int, error) {
	partitions, err := a.topicPartitions(ctx, topics)
	if err != nil {
		return nil, err
	}

	counts := make(map[string]int, len(partitions))
	for topic, ids := range partitions {
		counts[topic] = len(ids)
	}

	return counts, nil
}

// topicsHoldingRecords reports which of the given topics currently hold at least one
// retained record.
func (a *KafkaAdminClient) topicsHoldingRecords(
	ctx context.Context,
	partitions map[string][]int,
) (map[string]int64, error) {
	if len(partitions) == 0 {
		return nil, nil
	}

	bounds, err := a.offsetBounds(ctx, partitions)
	if err != nil {
		return nil, err
	}

	holding := make(map[string]int64, len(partitions))
	for topic, ids := range partitions {
		var records int64

		for _, id := range ids {
			bound, ok := bounds[topic][id]
			if !ok || bound.unavailable {
				// Unknown is treated as occupied. Guessing "empty" here would authorise a
				// growth that cannot be undone.
				records++

				continue
			}

			records += retainedRecords(bound.first, bound.end)
		}

		if records > 0 {
			holding[topic] = records
		}
	}

	return holding, nil
}

// missingTopics lists the requested topics the broker does not have.
func missingTopics(requested []string, present map[string][]int) []string {
	missing := make([]string, 0, len(requested))
	for _, topic := range requested {
		if _, exists := present[topic]; !exists {
			missing = append(missing, topic)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	return missing
}

// normalizeTopicList trims a topic list and drops blanks and duplicates, preserving the
// caller's order.
func normalizeTopicList(topics []string) []string {
	normalized := make([]string, 0, len(topics))
	seen := make(map[string]struct{}, len(topics))

	for _, topic := range topics {
		trimmed := strings.TrimSpace(topic)
		if trimmed == "" {
			continue
		}
		if _, duplicate := seen[trimmed]; duplicate {
			continue
		}
		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}

	if len(normalized) == 0 {
		return nil
	}

	return normalized
}

// ReconciliationBaseline is everything beyond the two counts that the verdict needs in
// order to be sound, gathered into one parameter.
type ReconciliationBaseline struct {
	// Purges is what retention has deleted from the outbox. Its Recorded field
	// distinguishes "nothing was purged" from "we cannot tell", and only the former
	// supports a conclusive verdict.
	Purges model.EventOutboxPurgeTotals

	// Coordinates is what the outbox claims about the broker, per partition. It is checked
	// against the report's live per-partition bounds, which is the step that turns the
	// reconciliation from an inference into a verification.
	Coordinates model.EventRecordCoordinateAudit
}

// TopicCatalogueReport is the EXISTENCE answer: which of the topics Blnk may write to
// the broker actually has.
type TopicCatalogueReport struct {
	// Expected is every topic Blnk may write to, across every owned prefix: each category
	// topic and each dead-letter sibling.
	Expected []string

	// Missing is the subset of Expected the broker does not have, in Expected's order.
	Missing []string

	// VerifiedAt is when the broker was read.
	VerifiedAt time.Time
}

// Complete reports whether the whole catalogue is present.
//
// Returns:
//   - bool: true when the broker has every expected topic.
func (r TopicCatalogueReport) Complete() bool {
	return len(r.Expected) > 0 && len(r.Missing) == 0
}

// VerifyTopicCatalogue reports which of the topics Blnk may write to are absent from
// the broker.
//
// Parameters:
//   - ctx context.Context: bounds the metadata read.
//
// Returns:
//   - TopicCatalogueReport: populated on success. Expected is always set, even on
//     error, so a caller can report what it was looking for.
//   - error: ErrKafkaAdminNotConfigured when no broker is configured, or a wrapped
//     broker error.
func (a *KafkaAdminClient) VerifyTopicCatalogue(ctx context.Context) (TopicCatalogueReport, error) {
	report := TopicCatalogueReport{
		Expected:   AllOwnedTopicsAcrossPrefixes(),
		VerifiedAt: time.Now().UTC(),
	}

	if err := a.ready(ctx); err != nil {
		return report, err
	}

	present, err := a.topicPartitions(ctx, report.Expected)
	if err != nil {
		return report, err
	}

	report.Missing = missingTopics(report.Expected, present)
	report.VerifiedAt = time.Now().UTC()

	return report, nil
}

// TopicCatalogueGate answers whether the relay's destination topics exist, caching the
// answer once they do and rate-limiting how often it asks while they do not.
type TopicCatalogueGate struct {
	// verify performs one ensure-and-verify pass. It is a field so a test can drive the gate
	// without a broker; in production newCatalogueVerifier builds it from configuration.
	verify func(ctx context.Context) (TopicCatalogueReport, error)

	// now is the clock, replaceable in-package, following the relay's own now field.
	now func() time.Time

	mu       sync.Mutex
	verified bool

	// nextProbeAt is when the gate will next talk to the broker. Zero means "now".
	nextProbeAt time.Time

	// probeInterval is the current gap, grown on each failure up to the cap.
	probeInterval time.Duration
}

// NewTopicCatalogueGate builds the gate the server role hands to the relay.
//
// Parameters:
//   - cfg *config.Configuration: read for the broker list and the topic geometry.
//
// Returns:
//   - *TopicCatalogueGate: ready to be passed to WithCatalogueGate.
func NewTopicCatalogueGate(cfg *config.Configuration) *TopicCatalogueGate {
	return &TopicCatalogueGate{
		verify:        newCatalogueVerifier(cfg),
		now:           time.Now,
		probeInterval: defaultCatalogueProbeInterval,
	}
}

// newCatalogueVerifier returns the ensure-and-verify pass the gate runs.
func newCatalogueVerifier(cfg *config.Configuration) func(ctx context.Context) (TopicCatalogueReport, error) {
	return func(ctx context.Context) (TopicCatalogueReport, error) {
		admin, err := NewKafkaAdmin(cfg)
		if err != nil {
			return TopicCatalogueReport{Expected: AllOwnedTopicsAcrossPrefixes()}, err
		}
		defer func() {
			if closeErr := admin.Close(); closeErr != nil {
				withLoggableCause(nil, closeErr).Warn(
					"closing the Kafka admin client after a topic-catalogue probe failed",
				)
			}
		}()

		report, err := admin.VerifyTopicCatalogue(ctx)
		if err != nil || report.Complete() {
			return report, err
		}

		// Something is missing, so try to create it. The assurance error is deliberately NOT
		// returned in place of the verification: what the caller needs to know is whether the
		// catalogue is complete NOW, and the authority on that is the second verification
		// below. A failed assurance whose gap another process then filled must still open the
		// gate.
		if _, ensureErr := admin.EnsureTopics(ctx); ensureErr != nil {
			withLoggableCause(logrus.WithField("missing_topics", report.Missing), ensureErr).Warn(
				"the event topic catalogue is incomplete and assuring it failed; the relay will not " +
					"claim outbox rows until every destination topic exists, so nothing is published " +
					"and nothing spends its retry budget on a topic that cannot accept it",
			)
		}

		return admin.VerifyTopicCatalogue(ctx)
	}
}

// Ready reports whether the relay may claim work.
//
// Parameters:
//   - ctx context.Context: cancellation is respected; the probe is additionally bounded
//     by catalogueProbeTimeout.
//
// Returns:
//   - error: nil when every expected topic exists.
func (g *TopicCatalogueGate) Ready(ctx context.Context) error {
	if g == nil {
		return nil
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.verified {
		return nil
	}

	if g.verify == nil {
		return errors.New(
			"the event topic catalogue gate has no verifier, so the relay cannot confirm that its " +
				"destination topics exist",
		)
	}

	now := g.now()
	if !g.nextProbeAt.IsZero() && now.Before(g.nextProbeAt) {
		// Inside the backoff window. The condition was already reported by the probe that
		// opened the window, so this is silent — and it costs nothing, which is what lets the
		// relay consult the gate on every tick.
		return errCatalogueNotVerified
	}

	probe, cancel := context.WithTimeout(ctx, catalogueProbeTimeout)
	defer cancel()

	report, err := g.verify(probe)
	if err == nil && report.Complete() {
		g.verified = true
		logrus.WithField("topics", len(report.Expected)).Info(
			"every event topic and dead-letter sibling exists; the event outbox relay may claim rows",
		)

		return nil
	}

	g.backOff(now)

	if err != nil {
		withLoggableCause(logrus.WithFields(logrus.Fields{
			"next_probe_in": g.probeInterval.String(),
			"expected":      len(report.Expected),
		}), err).Warn(
			"could not verify that the event topic catalogue exists, so the event outbox relay is " +
				"NOT claiming rows. Nothing is lost — the rows stay pending and no attempt is spent " +
				"on a destination that may not accept it — but nothing is published either until " +
				"this succeeds",
		)

		return err
	}

	logrus.WithFields(logrus.Fields{
		"missing_topics": report.Missing,
		"expected":       len(report.Expected),
		"next_probe_in":  g.probeInterval.String(),
	}).Warn(
		"event topics are missing from the broker, so the event outbox relay is NOT claiming rows. " +
			"Publishing to a topic that does not exist would fail every attempt and then fail to " +
			"dead-letter for the same reason, spending each row's retry budget for nothing. Provision " +
			"the topics — `make kafka_provision`, or let the server's own assurance succeed — and the " +
			"relay resumes on its own",
	)

	return fmt.Errorf("%w: %s", errCatalogueIncomplete, strings.Join(report.Missing, ", "))
}

// Verified reports whether the gate has confirmed the catalogue. It exists for tests and for a
// caller that wants to log the gate's state without probing.
//
// Returns:
//   - bool: true once a probe has seen the whole catalogue.
func (g *TopicCatalogueGate) Verified() bool {
	if g == nil {
		return false
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	return g.verified
}

// backOff schedules the next probe, doubling the interval up to the cap.
func (g *TopicCatalogueGate) backOff(now time.Time) {
	if g.probeInterval <= 0 {
		g.probeInterval = defaultCatalogueProbeInterval
	}

	g.nextProbeAt = now.Add(g.probeInterval)

	if next := g.probeInterval * 2; next <= maxCatalogueProbeInterval {
		g.probeInterval = next

		return
	}

	g.probeInterval = maxCatalogueProbeInterval
}

const (
	// defaultCatalogueProbeInterval is the shortest gap between two broker probes by the
	// topic-catalogue gate.
	defaultCatalogueProbeInterval = 5 * time.Second

	// maxCatalogueProbeInterval caps the gate's backoff.
	maxCatalogueProbeInterval = 60 * time.Second

	// catalogueProbeTimeout bounds one probe. A metadata read against a reachable broker
	// is milliseconds; this is generous enough for a loaded cluster and short enough that
	// a tick is never held up for long by an unreachable one.
	catalogueProbeTimeout = 10 * time.Second
)

var (
	// errCatalogueNotVerified is the silent refusal returned inside a backoff window. It
	// carries no detail because the detail was logged by the probe that opened the window.
	errCatalogueNotVerified = errors.New(
		"the event topic catalogue has not been verified yet, so the relay is not claiming rows",
	)

	// errCatalogueIncomplete reports that named topics are absent.
	errCatalogueIncomplete = errors.New("event topics are missing from the broker")
)
