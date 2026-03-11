package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import static com.linkedin.venice.pubsub.PubSubUtil.calculateSeekOffset;

import com.linkedin.venice.annotation.Threadsafe;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.PubSubPositionTypeRegistry;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.PubSubUtil;
import com.linkedin.venice.pubsub.adapter.kafka.TopicPartitionsOffsetsTracker;
import com.linkedin.venice.pubsub.adapter.kafka.common.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubMessageDeserializer;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicAuthorizationException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubUnsubscribedTopicPartitionException;
import java.io.IOException;
import java.nio.ByteBuffer;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
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
 * This class is thread safe. All operations on the internal {@link KafkaConsumer} and mutable state
 * are synchronized to ensure thread safety across concurrent access.
 */
@Threadsafe
public class ApacheKafkaConsumerAdapter implements PubSubConsumerAdapter {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaConsumerAdapter.class);
  private final Consumer<byte[], byte[]> kafkaConsumer;
  private final TopicPartitionsOffsetsTracker topicPartitionsOffsetsTracker;
  private final Map<TopicPartition, PubSubTopicPartition> assignments = new HashMap<>();
  private final PubSubMessageDeserializer pubSubMessageDeserializer;
  private final PubSubPositionTypeRegistry pubSubPositionTypeRegistry;
  private final ApacheKafkaConsumerConfig config;
  private final ReentrantLock consumerLock = new ReentrantLock(true); // Fair lock for FIFO ordering

  ApacheKafkaConsumerAdapter(ApacheKafkaConsumerConfig config) {
    this(new KafkaConsumer<>(config.getConsumerProperties()), config);
  }

  ApacheKafkaConsumerAdapter(Consumer<byte[], byte[]> consumer, ApacheKafkaConsumerConfig apacheKafkaConsumerConfig) {
    this.kafkaConsumer = Objects.requireNonNull(consumer, "Kafka consumer cannot be null");
    this.config = Objects.requireNonNull(apacheKafkaConsumerConfig, "ApacheKafkaConsumerConfig cannot be null");
    this.pubSubMessageDeserializer =
        Objects.requireNonNull(config.getPubSubMessageDeserializer(), "PubSubMessageDeserializer cannot be null");
    this.pubSubPositionTypeRegistry =
        Objects.requireNonNull(config.getPubSubPositionTypeRegistry(), "PubSubPositionTypeRegistry cannot be null");
    this.topicPartitionsOffsetsTracker = config.getTopicPartitionsOffsetsTracker();
    LOGGER.info(
        "Created ApacheKafkaConsumerAdapter with config: {} - isMetricsBasedOffsetCachingEnabled: {}",
        apacheKafkaConsumerConfig,
        topicPartitionsOffsetsTracker != null);
  }

  /**
   * Acquires the consumer lock using the configured default API timeout.
   *
   * <p>The caller must later release the lock via {@code consumerLock.unlock()} in a finally block.</p>
   *
   * @throws PubSubOpTimeoutException if the lock cannot be acquired within the timeout
   * @throws PubSubClientException if the thread is interrupted while waiting
   */
  private void acquireLockWithTimeout() {
    acquireLockWithTimeout(config.getDefaultApiTimeout());
  }

  /**
   * Acquires the consumer lock within the given timeout to avoid indefinite blocking.
   * Prevents concurrent access to the underlying KafkaConsumer.
   *
   * <p>The caller must later release the lock via {@code consumerLock.unlock()} in a finally block.</p>
   *
   * @param timeout maximum time to wait for the lock, must be positive
   * @throws PubSubOpTimeoutException if the lock cannot be acquired within the timeout
   * @throws PubSubClientException if the thread is interrupted while waiting
   */
  private void acquireLockWithTimeout(Duration timeout) {
    if (timeout == null) {
      throw new PubSubClientException("Timeout must not be null");
    }
    long timeoutMs = timeout.toMillis();
    if (timeoutMs <= 0) {
      throw new PubSubClientException("Timeout must be > 0 ms, was " + timeoutMs + " ms");
    }

    try {
      if (!consumerLock.tryLock(timeoutMs, TimeUnit.MILLISECONDS)) {
        throw new PubSubOpTimeoutException(
            "Failed to acquire consumer lock within " + timeoutMs + " ms. Thread=" + Thread.currentThread().getName());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PubSubClientException(
          "Interrupted while waiting for consumer lock. Thread=" + Thread.currentThread().getName(),
          e);
    }
  }

  /**
   * Releases the consumer lock safely.
   */
  private void releaseLock() {
    if (consumerLock.isHeldByCurrentThread()) {
      consumerLock.unlock();
    }
  }

  /**
   * Subscribes to a specified topic-partition if it is not already subscribed. If the topic-partition is already
   * subscribed, this method performs no action.
   *
   * The subscription uses the provided {@link PubSubPosition} to determine the starting offset for consumption.
   * If the position is {@link PubSubSymbolicPosition#EARLIEST}, the consumer will seek to the earliest available message.
   * If it is {@link PubSubSymbolicPosition#LATEST}, the consumer will seek to the latest offset. If an instance of
   * {@link ApacheKafkaOffsetPosition} is provided, the consumer will seek to the specified offset plus one.
   *
   * @param pubSubTopicPartition the topic-partition to subscribe to
   * @param lastReadPubSubPosition the last known position for the topic-partition
   * @throws IllegalArgumentException if lastReadPubSubPosition is null or not an instance of {@link ApacheKafkaOffsetPosition}
   * @throws PubSubTopicDoesNotExistException if the specified topic does not exist
   */
  @Override
  public void subscribe(
      @Nonnull PubSubTopicPartition pubSubTopicPartition,
      @Nonnull PubSubPosition lastReadPubSubPosition) {
    subscribe(pubSubTopicPartition, lastReadPubSubPosition, false);
  }

  @Override
  public void subscribe(
      @Nonnull PubSubTopicPartition pubSubTopicPartition,
      @Nonnull PubSubPosition position,
      boolean isInclusive) {
    LOGGER.info(
        "Requested subscription to topic-partition: {} with position: {} isInclusive: {}",
        pubSubTopicPartition,
        position,
        isInclusive);
    if (position == null) {
      LOGGER.error("Failed to subscribe to topic-partition: {} because position is null", pubSubTopicPartition);
      throw new IllegalArgumentException("Last read position cannot be null");
    }

    acquireLockWithTimeout();
    try {
      TopicPartition topicPartition = toKafkaTopicPartition(pubSubTopicPartition);
      if (kafkaConsumer.assignment().contains(topicPartition)) {
        LOGGER.warn(
            "Already subscribed to topic-partition:{}, ignoring subscription request with position: {}",
            pubSubTopicPartition,
            position);
        return;
      }

      validateTopicExistence(pubSubTopicPartition);

      List<TopicPartition> topicPartitionList = new ArrayList<>(kafkaConsumer.assignment());
      topicPartitionList.add(topicPartition);
      kafkaConsumer.assign(topicPartitionList);

      String logMessage;
      if (PubSubSymbolicPosition.EARLIEST.equals(position)) {
        kafkaConsumer.seekToBeginning(Collections.singletonList(topicPartition));
        logMessage = PubSubSymbolicPosition.EARLIEST + " (beginning)";
      } else if (PubSubSymbolicPosition.LATEST.equals(position)) {
        kafkaConsumer.seekToEnd(Collections.singletonList(topicPartition));
        logMessage = PubSubSymbolicPosition.LATEST + " (end)";
      } else if (position instanceof ApacheKafkaOffsetPosition) {
        ApacheKafkaOffsetPosition kafkaOffsetPosition = (ApacheKafkaOffsetPosition) position;
        long seekOffset = calculateSeekOffset(kafkaOffsetPosition.getInternalOffset(), isInclusive);
        kafkaConsumer.seek(topicPartition, seekOffset);
        logMessage = String.valueOf(seekOffset);
      } else {
        // N.B. This fallback path allows safe rollbacks where the PubSubPosition was written
        // by a newer PubSubClient (possibly using a non-default PubSubPosition implementation),
        // but is being consumed using the Kafka client.
        // In such cases, we attempt to use getNumericOffset() to preserve compatibility.
        // This behavior is temporary and will be deprecated once full enforcement is feasible.
        long seekOffset = calculateSeekOffset(position.getNumericOffset(), isInclusive);
        kafkaConsumer.seek(topicPartition, seekOffset);
        logMessage = String.valueOf(seekOffset);
      }

      assignments.put(topicPartition, pubSubTopicPartition);
      LOGGER.info("Subscribed to topic-partition: {} from position: {}", pubSubTopicPartition, logMessage);
    } finally {
      releaseLock();
    }
  }

  private TopicPartition toKafkaTopicPartition(PubSubTopicPartition topicPartition) {
    return new TopicPartition(topicPartition.getTopicName(), topicPartition.getPartitionNumber());
  }

  private void validateTopicExistence(PubSubTopicPartition pubSubTopicPartition) {
    if (config.shouldCheckTopicExistenceBeforeConsuming() && !isValidTopicPartition(pubSubTopicPartition)) {
      LOGGER.error("Cannot subscribe to topic-partition: {} because it does not exist", pubSubTopicPartition);
      throw new PubSubTopicDoesNotExistException(pubSubTopicPartition.getPubSubTopic());
    }
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
    acquireLockWithTimeout();
    try {
      String topic = pubSubTopicPartition.getTopicName();
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
    } finally {
      releaseLock();
    }
  }

  @Override
  public void batchUnsubscribe(Set<PubSubTopicPartition> pubSubTopicPartitionsToUnsubscribe) {
    acquireLockWithTimeout();
    try {
      // convert pubSubTopicPartitionsToUnsubscribe to a set of TopicPartition
      // additionally remove them from assignments and topicPartitionsOffsetsTracker
      Set<TopicPartition> topicPartitionsToUnsubscribe =
          pubSubTopicPartitionsToUnsubscribe.stream().map(pubSubTopicPartition -> {
            TopicPartition topicPartition =
                new TopicPartition(pubSubTopicPartition.getTopicName(), pubSubTopicPartition.getPartitionNumber());
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
    } finally {
      releaseLock();
    }
  }

  @Override
  public void resetOffset(PubSubTopicPartition pubSubTopicPartition) {
    acquireLockWithTimeout();
    try {
      TopicPartition topicPartition =
          new TopicPartition(pubSubTopicPartition.getTopicName(), pubSubTopicPartition.getPartitionNumber());
      if (!kafkaConsumer.assignment().contains(topicPartition)) {
        throw new PubSubUnsubscribedTopicPartitionException(pubSubTopicPartition);
      }
      kafkaConsumer.seekToBeginning(Collections.singletonList(topicPartition));
      LOGGER.info("Reset offset to beginning for topic-partition: {}", pubSubTopicPartition);
    } finally {
      releaseLock();
    }
  }

  @Override
  public Map<PubSubTopicPartition, List<DefaultPubSubMessage>> poll(long timeoutMs) {
    // The timeout is not respected when hitting UNKNOWN_TOPIC_OR_PARTITION and when the
    // fetcher.retrieveOffsetsByTimes call inside kafkaConsumer times out,
    // TODO: we may want to wrap this call in our own thread to enforce the timeout...
    int attemptCount = 1;

    while (attemptCount <= config.getConsumerPollRetryTimes() && !Thread.currentThread().isInterrupted()) {
      try {
        return pollInternal(timeoutMs);
      } catch (Exception e) {
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
        // Sleep outside the synchronized block to avoid blocking other threads
        if (config.getConsumerPollRetryBackoffMs() > 0) {
          try {
            Thread.sleep(config.getConsumerPollRetryBackoffMs());
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            // Here will still throw the actual exception thrown by internal consumer to make sure the stacktrace is
            // meaningful.
            throw new PubSubClientException("Consumer poll retry back off sleep got interrupted", e);
          }
        }
      } finally {
        attemptCount++;
      }
    }

    return Collections.emptyMap();
  }

  /**
   * Internal poll method that handles the actual Kafka consumer operations under lock.
   * This method is called by the public poll method which handles retry logic and sleep.
   */
  private Map<PubSubTopicPartition, List<DefaultPubSubMessage>> pollInternal(long timeoutMs) {
    acquireLockWithTimeout();
    try {
      ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(Duration.ofMillis(timeoutMs));
      Map<PubSubTopicPartition, List<DefaultPubSubMessage>> polledPubSubMessages =
          new HashMap<>(records.partitions().size());

      for (TopicPartition topicPartition: records.partitions()) {
        PubSubTopicPartition pubSubTopicPartition = assignments.get(topicPartition);
        List<ConsumerRecord<byte[], byte[]>> topicPartitionConsumerRecords = records.records(topicPartition);
        List<DefaultPubSubMessage> topicPartitionPubSubMessages = new ArrayList<>(topicPartitionConsumerRecords.size());
        for (ConsumerRecord<byte[], byte[]> consumerRecord: topicPartitionConsumerRecords) {
          topicPartitionPubSubMessages.add(deserialize(consumerRecord, pubSubTopicPartition));
        }
        polledPubSubMessages.put(pubSubTopicPartition, topicPartitionPubSubMessages);
      }

      if (topicPartitionsOffsetsTracker != null) {
        topicPartitionsOffsetsTracker.updateEndAndCurrentOffsets(records, kafkaConsumer);
      }

      return polledPubSubMessages;
    } finally {
      releaseLock();
    }
  }

  @Override
  public boolean hasAnySubscription() {
    acquireLockWithTimeout();
    try {
      return !kafkaConsumer.assignment().isEmpty();
    } finally {
      releaseLock();
    }
  }

  @Override
  public boolean hasSubscription(PubSubTopicPartition pubSubTopicPartition) {
    acquireLockWithTimeout();
    try {
      Objects.requireNonNull(pubSubTopicPartition, "PubSubTopicPartition cannot be null");
      TopicPartition tp =
          new TopicPartition(pubSubTopicPartition.getTopicName(), pubSubTopicPartition.getPartitionNumber());
      return kafkaConsumer.assignment().contains(tp);
    } finally {
      releaseLock();
    }
  }

  /**
   * If the partitions were not previously subscribed, this method is a no-op.
   */
  @Override
  public void pause(PubSubTopicPartition pubSubTopicPartition) {
    acquireLockWithTimeout();
    try {
      String topic = pubSubTopicPartition.getTopicName();
      int partition = pubSubTopicPartition.getPartitionNumber();
      TopicPartition tp = new TopicPartition(topic, partition);
      if (kafkaConsumer.assignment().contains(tp)) {
        kafkaConsumer.pause(Collections.singletonList(tp));
      }
    } finally {
      releaseLock();
    }
  }

  /**
   * If the partitions were not previously paused or if they were not subscribed at all, this method is a no-op.
   */
  @Override
  public void resume(PubSubTopicPartition pubSubTopicPartition) {
    acquireLockWithTimeout();
    try {
      String topic = pubSubTopicPartition.getTopicName();
      int partition = pubSubTopicPartition.getPartitionNumber();
      TopicPartition tp = new TopicPartition(topic, partition);
      if (kafkaConsumer.assignment().contains(tp)) {
        kafkaConsumer.resume(Collections.singletonList(tp));
      }
    } finally {
      releaseLock();
    }
  }

  @Override
  public Set<PubSubTopicPartition> getAssignment() {
    acquireLockWithTimeout();
    try {
      return new HashSet<>(assignments.values());
    } finally {
      releaseLock();
    }
  }

  @Override
  public void close() {
    acquireLockWithTimeout();
    try {
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
    } finally {
      releaseLock();
    }
  }

  @Override
  public long getOffsetLag(PubSubTopicPartition pubSubTopicPartition) {
    String topic = pubSubTopicPartition.getTopicName();
    int partition = pubSubTopicPartition.getPartitionNumber();
    return topicPartitionsOffsetsTracker != null ? topicPartitionsOffsetsTracker.getOffsetLag(topic, partition) : -1;
  }

  /**
   * Returns the latest offset for the given topic-partition. The latest offsets are derived from the lag metric
   * and may be outdated or imprecise.
   *
   * @param pubSubTopicPartition the topic-partition for which the latest offset is requested
   * @return the latest offset, or -1 if tracking is unavailable
   */
  @Override
  public long getLatestOffset(PubSubTopicPartition pubSubTopicPartition) {
    String topic = pubSubTopicPartition.getTopicName();
    int partition = pubSubTopicPartition.getPartitionNumber();
    return topicPartitionsOffsetsTracker != null ? topicPartitionsOffsetsTracker.getEndOffset(topic, partition) : -1;
  }

  @Override
  public PubSubPosition getPositionByTimestamp(PubSubTopicPartition pubSubTopicPartition, long timestamp) {
    return getPositionByTimestamp(pubSubTopicPartition, timestamp, null);
  }

  @Override
  public PubSubPosition getPositionByTimestamp(
      PubSubTopicPartition pubSubTopicPartition,
      long timestamp,
      Duration timeout) {
    acquireLockWithTimeout();
    try {
      TopicPartition topicPartition =
          new TopicPartition(pubSubTopicPartition.getTopicName(), pubSubTopicPartition.getPartitionNumber());
      Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetMap;
      if (timeout != null) {
        topicPartitionOffsetMap =
            this.kafkaConsumer.offsetsForTimes(Collections.singletonMap(topicPartition, timestamp), timeout);
      } else {
        topicPartitionOffsetMap =
            this.kafkaConsumer.offsetsForTimes(Collections.singletonMap(topicPartition, timestamp));
      }
      if (topicPartitionOffsetMap == null || topicPartitionOffsetMap.isEmpty()) {
        return null;
      }
      OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetMap.get(topicPartition);
      if (offsetAndTimestamp == null) {
        return null;
      }
      return new ApacheKafkaOffsetPosition(offsetAndTimestamp.offset());
    } catch (TimeoutException e) {
      String timeoutMsg = timeout != null ? " with timeout: " + timeout : "";
      throw new PubSubOpTimeoutException(
          "Timed out while getting offset for time: " + timestamp + " for: " + pubSubTopicPartition + timeoutMsg,
          e);
    } catch (Exception e) {
      String timeoutMsg = timeout != null ? " with timeout: " + timeout : "";
      throw new PubSubClientException(
          "Failed to fetch offset for time: " + timestamp + " for: " + pubSubTopicPartition + timeoutMsg,
          e);
    } finally {
      releaseLock();
    }
  }

  @Override
  public PubSubPosition beginningPosition(PubSubTopicPartition pubSubTopicPartition, Duration timeout) {
    return beginningPositions(Collections.singleton(pubSubTopicPartition), timeout).get(pubSubTopicPartition);
  }

  @Override
  public Map<PubSubTopicPartition, PubSubPosition> beginningPositions(
      Collection<PubSubTopicPartition> partitions,
      Duration timeout) {
    acquireLockWithTimeout(timeout);
    Map<TopicPartition, PubSubTopicPartition> partitionMapping = toKafkaTopicPartitionMap(partitions);
    try {
      Map<TopicPartition, Long> startOffsets = kafkaConsumer.beginningOffsets(partitionMapping.keySet(), timeout);
      Map<PubSubTopicPartition, PubSubPosition> offsets = new HashMap<>(startOffsets.size());
      for (Map.Entry<TopicPartition, Long> entry: startOffsets.entrySet()) {
        offsets.put(partitionMapping.get(entry.getKey()), new ApacheKafkaOffsetPosition(entry.getValue()));
      }
      return offsets;
    } catch (TimeoutException e) {
      throw new PubSubOpTimeoutException("Timed out while fetching start offsets for " + partitions, e);
    } catch (Exception e) {
      throw new PubSubClientException("Failed to fetch start offsets for " + partitions, e);
    } finally {
      releaseLock();
    }
  }

  private static Map<TopicPartition, PubSubTopicPartition> toKafkaTopicPartitionMap(
      Collection<PubSubTopicPartition> pubSubTopicPartitions) {
    Map<TopicPartition, PubSubTopicPartition> topicPartitionMap = new HashMap<>(pubSubTopicPartitions.size());
    for (PubSubTopicPartition pubSubTopicPartition: pubSubTopicPartitions) {
      topicPartitionMap.put(
          new TopicPartition(pubSubTopicPartition.getTopicName(), pubSubTopicPartition.getPartitionNumber()),
          pubSubTopicPartition);
    }
    return topicPartitionMap;
  }

  @Override
  public Map<PubSubTopicPartition, PubSubPosition> endPositions(
      Collection<PubSubTopicPartition> partitions,
      Duration timeout) {
    acquireLockWithTimeout(timeout);
    Map<TopicPartition, PubSubTopicPartition> pubSubTopicPartitionMapping = toKafkaTopicPartitionMap(partitions);
    try {
      Map<TopicPartition, Long> topicPartitionOffsetMap =
          this.kafkaConsumer.endOffsets(pubSubTopicPartitionMapping.keySet(), timeout);
      Map<PubSubTopicPartition, PubSubPosition> pubSubTopicPartitionOffsetMap =
          new HashMap<>(topicPartitionOffsetMap.size());
      for (Map.Entry<TopicPartition, Long> entry: topicPartitionOffsetMap.entrySet()) {
        PubSubTopicPartition pubSubTopicPartition = pubSubTopicPartitionMapping.get(entry.getKey());
        PubSubPosition endPosition =
            entry.getValue() != null ? new ApacheKafkaOffsetPosition(entry.getValue()) : PubSubSymbolicPosition.LATEST;
        pubSubTopicPartitionOffsetMap.put(pubSubTopicPartition, endPosition);
      }
      return pubSubTopicPartitionOffsetMap;
    } catch (TimeoutException e) {
      throw new PubSubOpTimeoutException("Timed out while fetching end offsets for " + partitions, e);
    } catch (Exception e) {
      throw new PubSubClientException("Failed to fetch end offsets for " + partitions, e);
    } finally {
      releaseLock();
    }
  }

  @Override
  public PubSubPosition endPosition(PubSubTopicPartition pubSubTopicPartition) {
    acquireLockWithTimeout();
    try {
      TopicPartition topicPartition =
          new TopicPartition(pubSubTopicPartition.getTopicName(), pubSubTopicPartition.getPartitionNumber());
      // Note: when timeout is not specified, the default request timeout ("request.timeout.ms")
      // is used, which is 30 seconds. For all other apis, the default request timeout is api
      // timeout ("default.api.timeout.ms"), which is 60 seconds.
      // To be consistent with other apis, use api timeout here.
      Map<TopicPartition, Long> topicPartitionOffsetMap =
          this.kafkaConsumer.endOffsets(Collections.singleton(topicPartition), config.getDefaultApiTimeout());
      Long endOffset = topicPartitionOffsetMap.get(topicPartition);
      return endOffset != null ? new ApacheKafkaOffsetPosition(endOffset) : PubSubSymbolicPosition.LATEST;
    } catch (TimeoutException e) {
      throw new PubSubOpTimeoutException("Timed out while fetching end position for " + pubSubTopicPartition, e);
    } catch (Exception e) {
      throw new PubSubClientException("Failed to fetch end position for " + pubSubTopicPartition, e);
    } finally {
      releaseLock();
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
    acquireLockWithTimeout();
    try {
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
              new PubSubTopicPartitionInfo(
                  topic,
                  partitionInfo.partition(),
                  partitionInfo.inSyncReplicas().length > 0));
        }
      }
      return pubSubTopicPartitionInfos;
    } finally {
      releaseLock();
    }
  }

  /**
   * Compares two {@link PubSubPosition} instances for a given {@link PubSubTopicPartition}.
   * <p>
   * Special symbolic positions are handled with the following order:
   * <ul>
   *   <li>{@link PubSubSymbolicPosition#EARLIEST} is considered the lowest possible position.</li>
   *   <li>{@link PubSubSymbolicPosition#LATEST} is considered the highest possible position.</li>
   * </ul>
   * If both positions are concrete (e.g., {@link ApacheKafkaOffsetPosition}), they must be of the same type and
   * will be compared based on their offset values.
   *
   * @param partition the topic partition context (not used in current implementation, but required for interface compatibility)
   * @param position1 the first position to compare (must not be null)
   * @param position2 the second position to compare (must not be null)
   * @return a negative value if {@code position1} is less than {@code position2}, zero if equal, or positive if greater
   * @throws IllegalArgumentException if either position is null or unsupported
   */
  @Override
  public long comparePositions(PubSubTopicPartition partition, PubSubPosition position1, PubSubPosition position2) {
    return positionDifference(partition, position1, position2);
  }

  @Override
  public long positionDifference(PubSubTopicPartition partition, PubSubPosition position1, PubSubPosition position2) {
    return PubSubUtil.computeOffsetDelta(partition, position1, position2, this);
  }

  @Override
  public PubSubPosition advancePosition(PubSubTopicPartition tp, PubSubPosition startInclusive, long n) {
    Objects.requireNonNull(tp, "tp");
    Objects.requireNonNull(startInclusive, "startInclusive");
    if (n < 0) {
      throw new IllegalArgumentException("n must be >= 0");
    }
    long startOffset = startInclusive.getNumericOffset();
    long targetOffset = Math.addExact(startOffset, n);
    return PubSubUtil.fromKafkaOffset(targetOffset);
  }

  @Override
  public PubSubPosition decodePosition(PubSubTopicPartition partition, int positionTypeId, ByteBuffer buffer) {
    if (buffer == null || buffer.remaining() == 0) {
      throw new VeniceException("Buffer cannot be null or empty for partition: " + partition);
    }
    if (positionTypeId != PubSubPositionTypeRegistry.APACHE_KAFKA_OFFSET_POSITION_TYPE_ID) {
      throw new VeniceException(
          "Position type ID: " + positionTypeId + " is not supported for partition: " + partition
              + ". Expected type ID: " + PubSubPositionTypeRegistry.APACHE_KAFKA_OFFSET_POSITION_TYPE_ID);
    }
    try {
      return new ApacheKafkaOffsetPosition(buffer);
    } catch (IOException e) {
      throw new VeniceException("Failed to decode position for partition: " + partition + " from buffer: " + buffer, e);
    }
  }

  /**
   * Deserialize the {@link ConsumerRecord} into {@link PubSubMessage}.
   * @param consumerRecord the {@link ConsumerRecord} to deserialize
   * @param topicPartition the {@link PubSubTopicPartition} of the {@link ConsumerRecord}
   * @return the deserialized {@link PubSubMessage}
   */
  private DefaultPubSubMessage deserialize(
      ConsumerRecord<byte[], byte[]> consumerRecord,
      PubSubTopicPartition topicPartition) {
    PubSubMessageHeaders pubSubMessageHeaders = new PubSubMessageHeaders();
    for (Header header: consumerRecord.headers()) {
      pubSubMessageHeaders.add(header.key(), header.value());
    }
    PubSubPosition pubSubPosition = ApacheKafkaOffsetPosition.of(consumerRecord.offset());
    return pubSubMessageDeserializer.deserialize(
        topicPartition,
        consumerRecord.key(),
        consumerRecord.value(),
        pubSubMessageHeaders,
        pubSubPosition,
        consumerRecord.timestamp());
  }
}
