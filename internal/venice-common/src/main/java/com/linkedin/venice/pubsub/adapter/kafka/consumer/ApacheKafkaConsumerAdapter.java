package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.adapter.kafka.TopicPartitionsOffsetsTracker;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.utils.VeniceProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.header.Header;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is not thread safe because of the internal {@link KafkaConsumer} being used.
 * backoff
 */
@NotThreadsafe
public class ApacheKafkaConsumerAdapter implements PubSubConsumerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaConsumerAdapter.class);

  public static final String CONSUMER_POLL_RETRY_TIMES_CONFIG = "consumer.poll.retry.times";
  public static final String CONSUMER_POLL_RETRY_BACKOFF_MS_CONFIG = "consumer.poll.retry.backoff.ms";
  private static final boolean DEFAULT_PARTITIONS_OFFSETS_COLLECTION_ENABLE = false;
  private static final int CONSUMER_POLL_RETRY_TIMES_DEFAULT = 3;
  private static final int CONSUMER_POLL_RETRY_BACKOFF_MS_DEFAULT = 0;

  private final Consumer<byte[], byte[]> kafkaConsumer;
  private final int consumerPollRetryTimes;
  private final int consumerPollRetryBackoffMs;
  private final TopicPartitionsOffsetsTracker topicPartitionsOffsetsTracker;

  private final Map<TopicPartition, PubSubTopicPartition> assignments;

  private final PubSubMessageDeserializer pubSubMessageDeserializer;

  public ApacheKafkaConsumerAdapter(Properties props, PubSubMessageDeserializer pubSubMessageDeserializer) {
    this(props, DEFAULT_PARTITIONS_OFFSETS_COLLECTION_ENABLE, pubSubMessageDeserializer);
  }

  public ApacheKafkaConsumerAdapter(
      Properties props,
      boolean isKafkaConsumerOffsetCollectionEnabled,
      PubSubMessageDeserializer pubSubMessageDeserializer) {
    this(
        new KafkaConsumer<>(props),
        new VeniceProperties(props),
        isKafkaConsumerOffsetCollectionEnabled,
        pubSubMessageDeserializer);
  }

  public ApacheKafkaConsumerAdapter(
      Consumer<byte[], byte[]> consumer,
      VeniceProperties props,
      boolean isKafkaConsumerOffsetCollectionEnabled,
      PubSubMessageDeserializer pubSubMessageDeserializer) {
    this.kafkaConsumer = consumer;
    this.consumerPollRetryTimes = props.getInt(CONSUMER_POLL_RETRY_TIMES_CONFIG, CONSUMER_POLL_RETRY_TIMES_DEFAULT);
    this.consumerPollRetryBackoffMs =
        props.getInt(CONSUMER_POLL_RETRY_BACKOFF_MS_CONFIG, CONSUMER_POLL_RETRY_BACKOFF_MS_DEFAULT);
    this.topicPartitionsOffsetsTracker =
        isKafkaConsumerOffsetCollectionEnabled ? new TopicPartitionsOffsetsTracker() : null;
    this.assignments = new HashMap<>();
    this.pubSubMessageDeserializer = pubSubMessageDeserializer;
    LOGGER.info("Consumer poll retry times: {}", this.consumerPollRetryTimes);
    LOGGER.info("Consumer poll retry back off in ms: {}", this.consumerPollRetryBackoffMs);
    LOGGER.info("Consumer offset collection enabled: {}", isKafkaConsumerOffsetCollectionEnabled);
  }

  private void seekNextOffset(TopicPartition topicPartition, long lastReadOffset) {
    // Kafka Consumer controls the default offset to start by the property
    // "auto.offset.reset" , it is set to "earliest" to start from the
    // beginning.

    // Venice would prefer to start from the beginning and using seekToBeginning
    // would have made it clearer. But that call always fail and can be used
    // only after the offsets are remembered for a partition in 0.9.0.2
    // TODO: Kafka has been upgraded to 0.11.*; we might be able to simply this function.
    if (lastReadOffset != OffsetRecord.LOWEST_OFFSET) {
      long nextReadOffset = lastReadOffset + 1;
      kafkaConsumer.seek(topicPartition, nextReadOffset);
    } else {
      // Considering the offset of the same consumer group could be persisted by some other consumer in Kafka.
      kafkaConsumer.seekToBeginning(Collections.singletonList(topicPartition));
    }
  }

  @Override
  public void subscribe(PubSubTopicPartition pubSubTopicPartition, long lastReadOffset) {
    String topic = pubSubTopicPartition.getPubSubTopic().getName();
    int partition = pubSubTopicPartition.getPartitionNumber();
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    Set<TopicPartition> topicPartitionSet = kafkaConsumer.assignment();
    if (!topicPartitionSet.contains(topicPartition)) {
      List<TopicPartition> topicPartitionList = new ArrayList<>(topicPartitionSet);
      topicPartitionList.add(topicPartition);
      kafkaConsumer.assign(topicPartitionList);
      seekNextOffset(topicPartition, lastReadOffset);
      assignments.put(topicPartition, pubSubTopicPartition);
      LOGGER.info("Subscribed to Topic: {} Partition: {} Offset: {}", topic, partition, lastReadOffset);
    } else {
      LOGGER
          .warn("Already subscribed on Topic: {} Partition: {}, ignore the request of subscription.", topic, partition);
    }
  }

  @Override
  public void unSubscribe(PubSubTopicPartition pubSubTopicPartition) {
    String topic = pubSubTopicPartition.getPubSubTopic().getName();
    int partition = pubSubTopicPartition.getPartitionNumber();
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    Set<TopicPartition> topicPartitionSet = kafkaConsumer.assignment();
    if (topicPartitionSet.contains(topicPartition)) {
      List<TopicPartition> topicPartitionList = new ArrayList<>(topicPartitionSet);
      if (topicPartitionList.remove(topicPartition)) {
        kafkaConsumer.assign(topicPartitionList);
      }
      assignments.remove(topicPartition);
    }
    if (topicPartitionsOffsetsTracker != null) {
      topicPartitionsOffsetsTracker.removeTrackedOffsets(topicPartition);
    }
  }

  @Override
  public void batchUnsubscribe(Set<PubSubTopicPartition> pubSubTopicPartitionSet) {
    pubSubTopicPartitionSet.forEach(
        pubSubTopicPartition -> assignments.remove(
            new TopicPartition(
                pubSubTopicPartition.getPubSubTopic().getName(),
                pubSubTopicPartition.getPartitionNumber())));
    Collection<TopicPartition> kafkaTopicPartitions = assignments.keySet();
    kafkaConsumer.assign(kafkaTopicPartitions);
  }

  @Override
  public void resetOffset(PubSubTopicPartition pubSubTopicPartition) {
    String topic = pubSubTopicPartition.getPubSubTopic().getName();
    int partition = pubSubTopicPartition.getPartitionNumber();
    if (!hasSubscription(pubSubTopicPartition)) {
      throw new UnsubscribedTopicPartitionException(pubSubTopicPartition);
    }
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    kafkaConsumer.seekToBeginning(Collections.singletonList(topicPartition));
  }

  @Override
  public Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> poll(long timeoutMs) {
    // The timeout is not respected when hitting UNKNOWN_TOPIC_OR_PARTITION and when the
    // fetcher.retrieveOffsetsByTimes call inside kafkaConsumer times out,
    // TODO: we may want to wrap this call in our own thread to enforce the timeout...
    int attemptCount = 1;
    ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> polledPubSubMessages =
        new HashMap<>();
    while (attemptCount <= consumerPollRetryTimes && !Thread.currentThread().isInterrupted()) {
      try {
        records = kafkaConsumer.poll(Duration.ofMillis(timeoutMs));
        for (TopicPartition topicPartition: records.partitions()) {
          PubSubTopicPartition pubSubTopicPartition = assignments.get(topicPartition);
          List<ConsumerRecord<byte[], byte[]>> topicPartitionConsumerRecords = records.records(topicPartition);
          List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> topicPartitionPubSubMessages =
              new ArrayList<>(topicPartitionConsumerRecords.size());
          for (ConsumerRecord<byte[], byte[]> consumerRecord: topicPartitionConsumerRecords) {
            PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> pubSubMessage =
                deserialize(consumerRecord, pubSubTopicPartition);
            topicPartitionPubSubMessages.add(pubSubMessage);
          }
          polledPubSubMessages.put(pubSubTopicPartition, topicPartitionPubSubMessages);
        }
        break;
      } catch (RetriableException e) {
        LOGGER.warn(
            "Retriable exception thrown when attempting to consume records from kafka, attempt {}/{}",
            attemptCount,
            consumerPollRetryTimes,
            e);
        if (attemptCount == consumerPollRetryTimes) {
          throw e;
        }
        try {
          if (consumerPollRetryBackoffMs > 0) {
            Thread.sleep(consumerPollRetryBackoffMs);
          }
        } catch (InterruptedException ie) {
          // Here will still throw the actual exception thrown by internal consumer to make sure the stacktrace is
          // meaningful.
          throw new VeniceException("Consumer poll retry back off sleep got interrupted", e);
        }
      } finally {
        attemptCount++;
      }
    }

    if (topicPartitionsOffsetsTracker != null) {
      topicPartitionsOffsetsTracker.updateEndAndCurrentOffsets(records, kafkaConsumer.metrics());
    }
    return polledPubSubMessages;
  }

  @Override
  public boolean hasAnySubscription() {
    return !kafkaConsumer.assignment().isEmpty();
  }

  @Override
  public boolean hasSubscription(PubSubTopicPartition pubSubTopicPartition) {
    String topic = pubSubTopicPartition.getPubSubTopic().getName();
    int partition = pubSubTopicPartition.getPartitionNumber();
    TopicPartition tp = new TopicPartition(topic, partition);
    return kafkaConsumer.assignment().contains(tp);
  }

  /**
   * If the partitions were not previously subscribed, this method is a no-op.
   */
  @Override
  public void pause(PubSubTopicPartition pubSubTopicPartition) {
    String topic = pubSubTopicPartition.getPubSubTopic().getName();
    int partition = pubSubTopicPartition.getPartitionNumber();
    TopicPartition tp = new TopicPartition(topic, partition);
    if (kafkaConsumer.assignment().contains(tp)) {
      kafkaConsumer.pause(Collections.singletonList(tp));
    }
  }

  /**
   * If the partitions were not previously paused or if they were not subscribed at all, this method is a no-op.
   */
  @Override
  public void resume(PubSubTopicPartition pubSubTopicPartition) {
    String topic = pubSubTopicPartition.getPubSubTopic().getName();
    int partition = pubSubTopicPartition.getPartitionNumber();
    TopicPartition tp = new TopicPartition(topic, partition);
    if (kafkaConsumer.assignment().contains(tp)) {
      kafkaConsumer.resume(Collections.singletonList(tp));
    }
  }

  @Override
  public Set<PubSubTopicPartition> getAssignment() {
    return new HashSet<>(assignments.values());
  }

  @Override
  public void close() {
    if (topicPartitionsOffsetsTracker != null) {
      topicPartitionsOffsetsTracker.clearAllOffsetState();
    }
    if (kafkaConsumer != null) {
      try {
        kafkaConsumer.close(Duration.ZERO);
      } catch (Exception e) {
        LOGGER.warn("{} threw an exception while closing.", kafkaConsumer.getClass().getSimpleName(), e);
      }
    }
  }

  @Override
  public long getOffsetLag(PubSubTopicPartition pubSubTopicPartition) {
    String topic = pubSubTopicPartition.getPubSubTopic().getName();
    int partition = pubSubTopicPartition.getPartitionNumber();
    return topicPartitionsOffsetsTracker != null ? topicPartitionsOffsetsTracker.getOffsetLag(topic, partition) : -1;
  }

  @Override
  public long getLatestOffset(PubSubTopicPartition pubSubTopicPartition) {
    String topic = pubSubTopicPartition.getPubSubTopic().getName();
    int partition = pubSubTopicPartition.getPartitionNumber();
    return topicPartitionsOffsetsTracker != null ? topicPartitionsOffsetsTracker.getEndOffset(topic, partition) : -1;
  }

  @Override
  public Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp, Duration timeout) {
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
    Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetMap =
        this.kafkaConsumer.offsetsForTimes(Collections.singletonMap(topicPartition, timestamp), timeout);
    if (topicPartitionOffsetMap.isEmpty()) {
      return -1L;
    }
    OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetMap.get(topicPartition);
    if (offsetAndTimestamp == null) {
      return null;
    }
    return offsetAndTimestamp.offset();
  }

  @Override
  public Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
    Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetMap =
        this.kafkaConsumer.offsetsForTimes(Collections.singletonMap(topicPartition, timestamp));
    if (topicPartitionOffsetMap.isEmpty()) {
      return -1L;
    }
    OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetMap.get(topicPartition);
    if (offsetAndTimestamp == null) {
      return null;
    }
    return offsetAndTimestamp.offset();
  }

  @Override
  public Long beginningOffset(PubSubTopicPartition pubSubTopicPartition, Duration timeout) {
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
    Map<TopicPartition, Long> topicPartitionOffset =
        this.kafkaConsumer.beginningOffsets(Collections.singleton(topicPartition), timeout);
    return topicPartitionOffset.get(topicPartition);
  }

  @Override
  public Map<PubSubTopicPartition, Long> endOffsets(Collection<PubSubTopicPartition> partitions, Duration timeout) {
    Map<TopicPartition, PubSubTopicPartition> pubSubTopicPartitionMapping = new HashMap<>(partitions.size());
    for (PubSubTopicPartition pubSubTopicPartition: partitions) {
      pubSubTopicPartitionMapping.put(
          new TopicPartition(
              pubSubTopicPartition.getPubSubTopic().getName(),
              pubSubTopicPartition.getPartitionNumber()),
          pubSubTopicPartition);
    }
    Map<PubSubTopicPartition, Long> pubSubTopicPartitionOffsetMap = new HashMap<>(partitions.size());
    Map<TopicPartition, Long> topicPartitionOffsetMap =
        this.kafkaConsumer.endOffsets(pubSubTopicPartitionMapping.keySet(), timeout);
    for (Map.Entry<TopicPartition, Long> entry: topicPartitionOffsetMap.entrySet()) {
      pubSubTopicPartitionOffsetMap.put(pubSubTopicPartitionMapping.get(entry.getKey()), entry.getValue());
    }
    return pubSubTopicPartitionOffsetMap;
  }

  @Override
  public Long endOffset(PubSubTopicPartition pubSubTopicPartition) {
    TopicPartition topicPartition =
        new TopicPartition(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
    Map<TopicPartition, Long> topicPartitionOffsetMap =
        this.kafkaConsumer.endOffsets(Collections.singleton(topicPartition));
    return topicPartitionOffsetMap.get(topicPartition);
  }

  @Override
  public List<PubSubTopicPartitionInfo> partitionsFor(PubSubTopic topic) {
    List<PartitionInfo> partitionInfos = this.kafkaConsumer.partitionsFor(topic.getName());
    if (partitionInfos == null) {
      return null;
    }
    List<PubSubTopicPartitionInfo> pubSubTopicPartitionInfos = new ArrayList<>(partitionInfos.size());
    for (PartitionInfo partitionInfo: partitionInfos) {
      if (partitionInfo.topic().equals(topic.getName())) {
        pubSubTopicPartitionInfos.add(
            new PubSubTopicPartitionInfo(topic, partitionInfo.partition(), partitionInfo.inSyncReplicas().length > 0));
      }
    }
    return pubSubTopicPartitionInfos;
  }

  /**
   * Deserialize the {@link ConsumerRecord} into {@link PubSubMessage}.
   * @param consumerRecord the {@link ConsumerRecord} to deserialize
   * @param topicPartition the {@link PubSubTopicPartition} of the {@link ConsumerRecord}
   * @return the deserialized {@link PubSubMessage}
   */
  private PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> deserialize(
      ConsumerRecord<byte[], byte[]> consumerRecord,
      PubSubTopicPartition topicPartition) {
    PubSubMessageHeaders pubSubMessageHeaders = new PubSubMessageHeaders();
    for (Header header: consumerRecord.headers()) {
      pubSubMessageHeaders.add(header.key(), header.value());
    }
    long position = consumerRecord.offset();
    return pubSubMessageDeserializer.deserialize(
        topicPartition,
        consumerRecord.key(),
        consumerRecord.value(),
        pubSubMessageHeaders,
        position,
        consumerRecord.timestamp());
  }
}
