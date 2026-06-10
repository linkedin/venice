package com.linkedin.venice.hadoop.snapshot;

import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.kafka.protocol.Delete;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.kafka.protocol.Update;
import com.linkedin.venice.kafka.protocol.enums.MessageType;
import com.linkedin.venice.message.KafkaKey;
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
import java.nio.ByteBuffer;
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
            SnapshotAtTRtRecord record = toRtRecord(message, coloId, cutoffTimestampMs);
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
          LOGGER.warn(
              "RT read of {} partition {} stopped early: consumed {} of {} expected records.",
              rtTopicName,
              partition,
              consumed,
              target);
        }
      }
    }
    LOGGER.info("Read {} RT records from {} (colo {}).", records.size(), rtTopicName, coloId);
    return records;
  }

  private static SnapshotAtTRtRecord toRtRecord(DefaultPubSubMessage message, int coloId, long cutoffTimestampMs) {
    KafkaKey key = message.getKey();
    if (key.isControlMessage() || key.isGlobalRtDiv()) {
      return null;
    }
    KafkaMessageEnvelope envelope = message.getValue();
    long writeTimestamp = envelope.producerMetadata.logicalTimestamp >= 0
        ? envelope.producerMetadata.logicalTimestamp
        : envelope.producerMetadata.messageTimestamp;
    if (cutoffTimestampMs > 0 && writeTimestamp > cutoffTimestampMs) {
      return null;
    }
    // Copy the key/value bytes out of the consumed message immediately: the message envelope's buffers may be
    // pooled/reused by the consumer on the next poll, and these records are processed only after the whole read
    // completes.
    ByteBuffer keyBytes = copy(ByteBuffer.wrap(key.getKey(), 0, key.getKeyLength()));
    MessageType messageType = MessageType.valueOf(envelope);
    switch (messageType) {
      case PUT:
        Put put = (Put) envelope.payloadUnion;
        return new SnapshotAtTRtRecord(
            SnapshotAtTRtRecord.Op.PUT,
            keyBytes,
            copy(put.putValue),
            put.schemaId,
            -1,
            writeTimestamp,
            coloId);
      case UPDATE:
        Update update = (Update) envelope.payloadUnion;
        return new SnapshotAtTRtRecord(
            SnapshotAtTRtRecord.Op.UPDATE,
            keyBytes,
            copy(update.updateValue),
            update.schemaId,
            update.updateSchemaId,
            writeTimestamp,
            coloId);
      case DELETE:
        Delete delete = (Delete) envelope.payloadUnion;
        return new SnapshotAtTRtRecord(
            SnapshotAtTRtRecord.Op.DELETE,
            keyBytes,
            null,
            delete.schemaId,
            -1,
            writeTimestamp,
            coloId);
      default:
        return null;
    }
  }

  /** Copy a buffer's remaining content into a fresh, independently-owned ByteBuffer. */
  private static ByteBuffer copy(ByteBuffer source) {
    if (source == null) {
      return null;
    }
    ByteBuffer duplicate = source.duplicate();
    byte[] bytes = new byte[duplicate.remaining()];
    duplicate.get(bytes);
    return ByteBuffer.wrap(bytes);
  }
}
