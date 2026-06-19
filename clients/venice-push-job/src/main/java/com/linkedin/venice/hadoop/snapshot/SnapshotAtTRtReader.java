package com.linkedin.venice.hadoop.snapshot;

import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubClientsFactory;
import com.linkedin.venice.pubsub.PubSubConsumerAdapterContext;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicPartitionImpl;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;
import com.linkedin.venice.pubsub.manager.TopicManagerContext;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Reads a single region's real-time (RT) topic and normalizes its records for the snapshot-at-T merge. Each
 * record is tagged with the caller-supplied {@code coloId} (the region the topic belongs to), so the merger can
 * do Active-Active conflict resolution across regions. Call once per region and concatenate the results.
 *
 * <p>It reads exactly the start-to-end record count of each partition (so the read is deterministic, not a flaky
 * poll-until-idle drain), which corresponds to "up to the current tail" (T = read time). Records whose write
 * timestamp exceeds {@code cutoffTimestampMs} are filtered out, giving event-time bounding when a {@code T}
 * earlier than now is requested.
 */
public class SnapshotAtTRtReader {
  private static final Logger LOGGER = LogManager.getLogger(SnapshotAtTRtReader.class);
  private static final long POLL_TIMEOUT_MS = 5_000;
  private static final int MAX_EMPTY_POLLS = 10;

  /**
   * @param consumerProps pubsub client config (selects the consumer/admin adapter implementation)
   * @param brokerAddress the region's pubsub broker address
   * @param rtTopicName the store's RT topic name
   * @param partitionCount number of partitions of the RT topic
   * @param cutoffTimestampMs include only records with write timestamp &le; this; {@code <= 0} means no bound
   * @param coloId the colo id to tag this region's records with (for cross-region conflict resolution)
   */
  public List<SnapshotAtTRtRecord> readRegion(
      VeniceProperties consumerProps,
      String brokerAddress,
      String rtTopicName,
      int partitionCount,
      long cutoffTimestampMs,
      int coloId) {
    PubSubClientsFactory clientsFactory = new PubSubClientsFactory(consumerProps);
    PubSubTopicRepository topicRepository = new PubSubTopicRepository();
    PubSubTopic rtTopic = topicRepository.getTopic(rtTopicName);
    List<SnapshotAtTRtRecord> records = new ArrayList<>();

    TopicManagerContext topicManagerContext =
        new TopicManagerContext.Builder().setPubSubPropertiesSupplier(k -> consumerProps)
            .setPubSubTopicRepository(topicRepository)
            .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(consumerProps))
            .setPubSubAdminAdapterFactory(clientsFactory.getAdminAdapterFactory())
            .setPubSubConsumerAdapterFactory(clientsFactory.getConsumerAdapterFactory())
            .setTopicMetadataFetcherThreadPoolSize(1)
            .setTopicMetadataFetcherConsumerPoolSize(1)
            .setVeniceComponent(VeniceComponent.PUSH_JOB)
            .build();

    PubSubConsumerAdapterContext consumerContext =
        new PubSubConsumerAdapterContext.Builder().setConsumerName("snapshot-at-t-rt-reader-colo-" + coloId)
            .setPubSubBrokerAddress(brokerAddress)
            .setVeniceProperties(consumerProps)
            .setPubSubTopicRepository(topicRepository)
            .setPubSubMessageDeserializer(PubSubMessageDeserializer.createDefaultDeserializer())
            .setPubSubPositionTypeRegistry(PubSubPositionTypeRegistry.fromPropertiesOrDefault(consumerProps))
            .build();

    try (
        TopicManager topicManager =
            new TopicManagerRepository(topicManagerContext, brokerAddress).getLocalTopicManager();
        PubSubConsumerAdapter consumer = clientsFactory.getConsumerAdapterFactory().create(consumerContext)) {
      // The separate RT topic may not exist yet for a store that has it enabled but has taken no incremental
      // push; an absent topic genuinely has no records to merge, so return empty rather than failing the read.
      if (!topicManager.containsTopic(rtTopic)) {
        LOGGER.info(
            "RT topic {} does not exist on broker {} (colo {}); nothing to read.",
            rtTopicName,
            brokerAddress,
            coloId);
        return records;
      }
      Map<PubSubTopicPartition, PubSubPosition> startPositions =
          topicManager.getStartPositionsForTopicWithRetries(rtTopic);
      Map<PubSubTopicPartition, PubSubPosition> endPositions = topicManager.getEndPositionsForTopicWithRetries(rtTopic);

      for (int partition = 0; partition < partitionCount; partition++) {
        PubSubTopicPartition topicPartition = new PubSubTopicPartitionImpl(rtTopic, partition);
        PubSubPosition start = startPositions.get(topicPartition);
        PubSubPosition end = endPositions.get(topicPartition);
        if (start == null || end == null) {
          continue;
        }
        long target = topicManager.diffPosition(topicPartition, end, start);
        if (target <= 0) {
          continue;
        }
        consumer.subscribe(topicPartition, start, true);
        long consumed = 0;
        int emptyPolls = 0;
        while (consumed < target && emptyPolls < MAX_EMPTY_POLLS) {
          Map<PubSubTopicPartition, List<DefaultPubSubMessage>> polled = consumer.poll(POLL_TIMEOUT_MS);
          List<DefaultPubSubMessage> messages = polled.get(topicPartition);
          if (messages == null || messages.isEmpty()) {
            emptyPolls++;
            continue;
          }
          emptyPolls = 0;
          for (DefaultPubSubMessage message: messages) {
            consumed++;
            SnapshotAtTRtRecord record = SnapshotAtTRtRecordDecoder.decode(message, coloId, cutoffTimestampMs);
            if (record != null) {
              records.add(record);
            }
            if (consumed >= target) {
              break;
            }
          }
        }
        consumer.unSubscribe(topicPartition);
        if (consumed < target) {
          throw new VeniceException(
              String.format(
                  "Snapshot-at-T RT read of %s partition %d is incomplete: consumed %d of %d expected records. "
                      + "Aborting to avoid producing a partial merged dataset (potential data loss).",
                  rtTopicName,
                  partition,
                  consumed,
                  target));
        }
      }
    }
    LOGGER.info("Read {} RT records from {} (colo {}).", records.size(), rtTopicName, coloId);
    return records;
  }
}
