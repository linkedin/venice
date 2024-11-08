package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import com.linkedin.venice.annotation.NotThreadsafe;
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
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicAuthorizationException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubUnsubscribedTopicPartitionException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Header;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is not thread safe because of the internal {@link KafkaConsumer} is not thread safe.
 * It is the responsibility of the caller to ensure that the methods are called in a thread safe manner.
 */
@NotThreadsafe
public class ApacheKafkaConsumerAdapter implements PubSubConsumerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaConsumerAdapter.class);

  private final Consumer<byte[], byte[]> kafkaConsumer;
  private final TopicPartitionsOffsetsTracker topicPartitionsOffsetsTracker;
  private final Map<TopicPartition, PubSubTopicPartition> assignments = new HashMap<>();
  private final PubSubMessageDeserializer pubSubMessageDeserializer;
  private final ApacheKafkaConsumerConfig config;

  ApacheKafkaConsumerAdapter(
      ApacheKafkaConsumerConfig config,
      PubSubMessageDeserializer pubSubMessageDeserializer,
      boolean isKafkaConsumerOffsetCollectionEnabled) {
    this(
        new KafkaConsumer<>(config.getConsumerProperties()),
        config,
        pubSubMessageDeserializer,
        isKafkaConsumerOffsetCollectionEnabled ? new TopicPartitionsOffsetsTracker() : null);
  }

  ApacheKafkaConsumerAdapter(
      Consumer<byte[], byte[]> consumer,
      ApacheKafkaConsumerConfig apacheKafkaConsumerConfig,
      PubSubMessageDeserializer pubSubMessageDeserializer,
      TopicPartitionsOffsetsTracker topicPartitionsOffsetsTracker) {
    this.kafkaConsumer = Objects.requireNonNull(consumer, "Kafka consumer cannot be null");
    this.config = Objects.requireNonNull(apacheKafkaConsumerConfig, "ApacheKafkaConsumerConfig cannot be null");
    this.pubSubMessageDeserializer =
        Objects.requireNonNull(pubSubMessageDeserializer, "PubSubMessageDeserializer cannot be null");
    this.topicPartitionsOffsetsTracker = topicPartitionsOffsetsTracker;
    LOGGER.info(
        "Created ApacheKafkaConsumerAdapter with config: {} - isMetricsBasedOffsetCachingEnabled: {}",
        apacheKafkaConsumerConfig,
        topicPartitionsOffsetsTracker != null);
  }

  /**
   * Subscribe to a topic-partition if not already subscribed. If the topic-partition is already subscribed, this
   * method is a no-op. This method requires the topic-partition to exist.
   * @param pubSubTopicPartition the topic-partition to subscribe to
   * @param lastReadOffset the last read offset for the topic-partition
   * @throws IllegalArgumentException if the topic-partition is null or the partition number is negative
   * @throws PubSubTopicDoesNotExistException if the topic does not exist
   */
  @Override
  public void subscribe(PubSubTopicPartition pubSubTopicPartition, long lastReadOffset) {
    String topic = pubSubTopicPartition.getPubSubTopic().getName();
    int partition = pubSubTopicPartition.getPartitionNumber();
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    Set<TopicPartition> topicPartitionSet = kafkaConsumer.assignment();
    if (topicPartitionSet.contains(topicPartition)) {
      LOGGER.warn("Already subscribed to topic-partition:{}, ignoring subscription request", pubSubTopicPartition);
      return;
    }

    // Check if the topic-partition exists
    if (config.shouldCheckTopicExistenceBeforeConsuming() && !isValidTopicPartition(pubSubTopicPartition)) {
      LOGGER.error("Cannot subscribe to topic-partition: {} because it does not exist", pubSubTopicPartition);
      throw new PubSubTopicDoesNotExistException(pubSubTopicPartition.getPubSubTopic());
    }

    List<TopicPartition> topicPartitionList = new ArrayList<>(topicPartitionSet);
    topicPartitionList.add(topicPartition);
    kafkaConsumer.assign(topicPartitionList); // add the topic-partition to the subscription
    // Use the last read offset to seek to the next offset to read.
    long consumptionStartOffset = lastReadOffset <= OffsetRecord.LOWEST_OFFSET ? 0 : lastReadOffset + 1;
    if (lastReadOffset <= OffsetRecord.LOWEST_OFFSET) {
      if (lastReadOffset < OffsetRecord.LOWEST_OFFSET) {
        LOGGER.warn(
            "Last read offset: {} for topic-partition: {} is less than the lowest offset: {}, seeking to beginning."
                + " This may indicate an off-by-one error.",
            lastReadOffset,
            pubSubTopicPartition,
            OffsetRecord.LOWEST_OFFSET);
      }
      kafkaConsumer.seekToBeginning(Collections.singletonList(topicPartition));
    } else {
      kafkaConsumer.seek(topicPartition, consumptionStartOffset);
    }
    assignments.put(topicPartition, pubSubTopicPartition);
    LOGGER.info(
        "Subscribed to topic-partition: {} at offset: {} and last read offset was: {}",
        pubSubTopicPartition,
        consumptionStartOffset,
        lastReadOffset);
  }

  // visible for testing
  boolean isValidTopicPartition(PubSubTopicPartition pubSubTopicPartition) {
    if (pubSubTopicPartition == null) {
      throw new IllegalArgumentException("PubSubTopicPartition cannot be null");
    }
    if (pubSubTopicPartition.getPartitionNumber() < 0) {
      throw new IllegalArgumentException("Partition number cannot be negative");
    }

    List<PubSubTopicPartitionInfo> topicPartitionInfos;
    int retries = config.getTopicQueryRetryTimes();
    int attempt = 0;
    while (attempt++ < retries) {
      try {
        topicPartitionInfos = partitionsFor(pubSubTopicPartition.getPubSubTopic());
        if (topicPartitionInfos != null && !topicPartitionInfos.isEmpty()
            && pubSubTopicPartition.getPartitionNumber() < topicPartitionInfos.size()) {
          return true;
        }
      } catch (PubSubClientRetriableException e) {
        LOGGER.warn(
            "Exception thrown when attempting to validate topic-partition: {}, attempt {}/{}",
            pubSubTopicPartition,
            attempt,
            retries,
            e);
      }
      try {
        Thread.sleep(Math.max(1, config.getTopicQueryRetryIntervalMs()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new PubSubClientException(
            "Interrupted while waiting for validation of topic-partition: " + pubSubTopicPartition,
            e);
      }
    }
    return false;
  }

  @Override
  public void unSubscribe(PubSubTopicPartition pubSubTopicPartition) {
    String topic = pubSubTopicPartition.getPubSubTopic().getName();
    int partition = pubSubTopicPartition.getPartitionNumber();
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    Set<TopicPartition> topicPartitionSet = kafkaConsumer.assignment();
    boolean isSubscribed = topicPartitionSet.contains(topicPartition);
    if (isSubscribed) {
      List<TopicPartition> topicPartitionList = new ArrayList<>(topicPartitionSet);
      if (topicPartitionList.remove(topicPartition)) {
        kafkaConsumer.assign(topicPartitionList);
      }
      assignments.remove(topicPartition);
    }
    if (topicPartitionsOffsetsTracker != null) {
      topicPartitionsOffsetsTracker.removeTrackedOffsets(topicPartition);
    }
    LOGGER.info("Topic-partition: {} {} unsubscribed", pubSubTopicPartition, isSubscribed ? "is" : "was already");
  }

  @Override
  public void batchUnsubscribe(Set<PubSubTopicPartition> pubSubTopicPartitionsToUnsubscribe) {
    // convert pubSubTopicPartitionsToUnsubscribe to a set of TopicPartition
    // additionally remove them from assignments and topicPartitionsOffsetsTracker
    Set<TopicPartition> topicPartitionsToUnsubscribe =
        pubSubTopicPartitionsToUnsubscribe.stream().map(pubSubTopicPartition -> {
          TopicPartition topicPartition = new TopicPartition(
              pubSubTopicPartition.getPubSubTopic().getName(),
              pubSubTopicPartition.getPartitionNumber());
          assignments.remove(topicPartition);
          if (topicPartitionsOffsetsTracker != null) {
            topicPartitionsOffsetsTracker.removeTrackedOffsets(topicPartition);
          }
          return topicPartition;
        }).collect(Collectors.toSet());

    Collection<TopicPartition> currentAssignments = new HashSet<>(kafkaConsumer.assignment());
    currentAssignments.removeAll(topicPartitionsToUnsubscribe);
    kafkaConsumer.assign(currentAssignments);
    LOGGER.info("Topic-partitions: {} unsubscribed", pubSubTopicPartitionsToUnsubscribe);
  }

  @Override
  public void resetOffset(PubSubTopicPartition pubSubTopicPartition) {
    String topic = pubSubTopicPartition.getPubSubTopic().getName();
    int partition = pubSubTopicPartition.getPartitionNumber();
    if (!hasSubscription(pubSubTopicPartition)) {
      throw new PubSubUnsubscribedTopicPartitionException(pubSubTopicPartition);
    }
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    kafkaConsumer.seekToBeginning(Collections.singletonList(topicPartition));
    LOGGER.info("Reset offset to beginning for topic-partition: {}", pubSubTopicPartition);
  }

  @Override
  public Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> poll(long timeoutMs) {
    // The timeout is not respected when hitting UNKNOWN_TOPIC_OR_PARTITION and when the
    // fetcher.retrieveOffsetsByTimes call inside kafkaConsumer times out,
    // TODO: we may want to wrap this call in our own thread to enforce the timeout...
    int attemptCount = 1;
    ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
    Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> polledPubSubMessages =
        Collections.emptyMap();
    while (attemptCount <= config.getConsumerPollRetryTimes() && !Thread.currentThread().isInterrupted()) {
      try {
        records = kafkaConsumer.poll(Duration.ofMillis(timeoutMs));
        polledPubSubMessages = new HashMap<>(records.partitions().size());
        for (TopicPartition topicPartition: records.partitions()) {
          PubSubTopicPartition pubSubTopicPartition = assignments.get(topicPartition);
          List<ConsumerRecord<byte[], byte[]>> topicPartitionConsumerRecords = records.records(topicPartition);
          List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>> topicPartitionPubSubMessages =
              new ArrayList<>(topicPartitionConsumerRecords.size());
          for (ConsumerRecord<byte[], byte[]> consumerRecord: topicPartitionConsumerRecords) {
            topicPartitionPubSubMessages.add(deserialize(consumerRecord, pubSubTopicPartition));
          }
          polledPubSubMessages.put(pubSubTopicPartition, topicPartitionPubSubMessages);
        }
        break;
      } catch (RetriableException e) {
        LOGGER.warn(
            "Retriable exception thrown when attempting to consume records from kafka, attempt {}/{}",
            attemptCount,
            config.getConsumerPollRetryTimes(),
            e);
        if (attemptCount == config.getConsumerPollRetryTimes()) {
          throw new PubSubClientRetriableException(
              "Retriable exception thrown when attempting to consume records from kafka, attempt " + attemptCount + "/"
                  + config.getConsumerPollRetryTimes(),
              e);
        }
        try {
          if (config.getConsumerPollRetryBackoffMs() > 0) {
            Thread.sleep(config.getConsumerPollRetryBackoffMs());
          }
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          // Here will still throw the actual exception thrown by internal consumer to make sure the stacktrace is
          // meaningful.
          throw new PubSubClientException("Consumer poll retry back off sleep got interrupted", e);
        }
      } finally {
        attemptCount++;
      }
    }

    if (topicPartitionsOffsetsTracker != null) {
      topicPartitionsOffsetsTracker.updateEndAndCurrentOffsets(records, kafkaConsumer);
    }
    return polledPubSubMessages;
  }

  @Override
  public boolean hasAnySubscription() {
    return !kafkaConsumer.assignment().isEmpty();
  }

  @Override
  public boolean hasSubscription(PubSubTopicPartition pubSubTopicPartition) {
    pubSubTopicPartition = Objects.requireNonNull(pubSubTopicPartition, "PubSubTopicPartition cannot be null");
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
    if (pubSubMessageDeserializer != null) {
      pubSubMessageDeserializer.close();
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

  /**
   * @return get the offset of the first message with timestamp greater than or equal to the target timestamp.
   *          {@code null} will be returned for the partition if there is no such message.
   */
  @Override
  public Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp, Duration timeout) {
    try {
      TopicPartition topicPartition =
          new TopicPartition(pubSubTopicPartition.getTopicName(), pubSubTopicPartition.getPartitionNumber());
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
    } catch (TimeoutException e) {
      throw new PubSubOpTimeoutException(
          "Timed out while getting offset for time: " + timestamp + " for: " + pubSubTopicPartition + " with timeout: "
              + timeout,
          e);
    } catch (Exception e) {
      throw new PubSubClientException(
          "Failed to fetch offset for time: " + timestamp + " for: " + pubSubTopicPartition + " with timeout: "
              + timeout,
          e);
    }
  }

  @Override
  public Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    try {
      TopicPartition topicPartition = new TopicPartition(
          pubSubTopicPartition.getPubSubTopic().getName(),
          pubSubTopicPartition.getPartitionNumber());
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
    } catch (TimeoutException e) {
      throw new PubSubOpTimeoutException(
          "Timed out while getting offset for time: " + timestamp + " for: " + pubSubTopicPartition,
          e);
    } catch (Exception e) {
      throw new PubSubClientException(
          "Failed to fetch offset for time: " + timestamp + " for: " + pubSubTopicPartition,
          e);
    }
  }

  @Override
  public Long beginningOffset(PubSubTopicPartition pubSubTopicPartition, Duration timeout) {
    TopicPartition kafkaTp =
        new TopicPartition(pubSubTopicPartition.getPubSubTopic().getName(), pubSubTopicPartition.getPartitionNumber());
    try {
      return this.kafkaConsumer.beginningOffsets(Collections.singleton(kafkaTp), timeout).get(kafkaTp);
    } catch (TimeoutException e) {
      throw new PubSubOpTimeoutException("Timed out while getting beginning offset for " + kafkaTp, e);
    } catch (Exception e) {
      throw new PubSubClientException("Exception while getting beginning offset for " + kafkaTp, e);
    }
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
    try {
      Map<TopicPartition, Long> topicPartitionOffsetMap =
          this.kafkaConsumer.endOffsets(pubSubTopicPartitionMapping.keySet(), timeout);
      Map<PubSubTopicPartition, Long> pubSubTopicPartitionOffsetMap = new HashMap<>(topicPartitionOffsetMap.size());
      for (Map.Entry<TopicPartition, Long> entry: topicPartitionOffsetMap.entrySet()) {
        pubSubTopicPartitionOffsetMap.put(pubSubTopicPartitionMapping.get(entry.getKey()), entry.getValue());
      }
      return pubSubTopicPartitionOffsetMap;
    } catch (TimeoutException e) {
      throw new PubSubOpTimeoutException("Timed out while fetching end offsets for " + partitions, e);
    } catch (Exception e) {
      throw new PubSubClientException("Failed to fetch end offsets for " + partitions, e);
    }
  }

  @Override
  public Long endOffset(PubSubTopicPartition pubSubTopicPartition) {
    try {
      TopicPartition topicPartition = new TopicPartition(
          pubSubTopicPartition.getPubSubTopic().getName(),
          pubSubTopicPartition.getPartitionNumber());
      // Note: when timeout is not specified, the default request timeout ("request.timeout.ms")
      // is used, which is 30 seconds. For all other apis, the default request timeout is api
      // timeout ("default.api.timeout.ms"), which is 60 seconds.
      // To be consistent with other apis, use api timeout here.
      Map<TopicPartition, Long> topicPartitionOffsetMap =
          this.kafkaConsumer.endOffsets(Collections.singleton(topicPartition), config.getDefaultApiTimeout());
      return topicPartitionOffsetMap.get(topicPartition);
    } catch (TimeoutException e) {
      throw new PubSubOpTimeoutException("Timed out while fetching end offset for " + pubSubTopicPartition, e);
    } catch (Exception e) {
      throw new PubSubClientException("Failed to fetch end offset for " + pubSubTopicPartition, e);
    }
  }

  /**
   * Retrieves the list of partitions associated with a given Pub-Sub topic.
   *
   * @param topic The Pub-Sub topic for which partition information is requested.
   * @return A list of {@link PubSubTopicPartitionInfo} representing the partitions of the topic,
   *         or {@code null} if the topic does not exist.
   */
  @Override
  public List<PubSubTopicPartitionInfo> partitionsFor(PubSubTopic topic) {
    List<PartitionInfo> partitionInfos;
    try {
      partitionInfos = this.kafkaConsumer.partitionsFor(topic.getName());
    } catch (RetriableException e) {
      throw new PubSubClientRetriableException(
          "Retriable exception thrown when attempting to get partitions for topic: " + topic,
          e);
    } catch (AuthorizationException | AuthenticationException e) {
      throw new PubSubTopicAuthorizationException(
          "Authorization exception thrown when attempting to get partitions for topic: " + topic,
          e);
    } catch (Exception e) {
      if (e instanceof InterruptException) {
        Thread.currentThread().interrupt();
      }
      throw new PubSubClientException("Exception thrown when attempting to get partitions for topic: " + topic, e);
    }

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
