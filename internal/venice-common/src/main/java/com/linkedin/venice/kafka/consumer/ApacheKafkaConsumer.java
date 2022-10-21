package com.linkedin.venice.kafka.consumer;

import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.utils.VeniceProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class is not thread safe because of the internal {@link KafkaConsumer} being used.
 * backoff
 */
@NotThreadsafe
public class ApacheKafkaConsumer implements KafkaConsumerWrapper {
  private static final Logger LOGGER = LogManager.getLogger(ApacheKafkaConsumer.class);

  public static final String CONSUMER_POLL_RETRY_TIMES_CONFIG = "consumer.poll.retry.times";
  public static final String CONSUMER_POLL_RETRY_BACKOFF_MS_CONFIG = "consumer.poll.retry.backoff.ms";
  private static final boolean DEFAULT_PARTITIONS_OFFSETS_COLLECTION_ENABLE = false;
  private static final int CONSUMER_POLL_RETRY_TIMES_DEFAULT = 3;
  private static final int CONSUMER_POLL_RETRY_BACKOFF_MS_DEFAULT = 0;

  private final Consumer<KafkaKey, KafkaMessageEnvelope> kafkaConsumer;
  private final int consumerPollRetryTimes;
  private final int consumerPollRetryBackoffMs;
  private final Optional<TopicPartitionsOffsetsTracker> topicPartitionsOffsetsTracker;

  public ApacheKafkaConsumer(Properties props) {
    this(props, DEFAULT_PARTITIONS_OFFSETS_COLLECTION_ENABLE);
  }

  public ApacheKafkaConsumer(Properties props, boolean isKafkaConsumerOffsetCollectionEnabled) {
    this(new KafkaConsumer<>(props), new VeniceProperties(props), isKafkaConsumerOffsetCollectionEnabled);
  }

  public ApacheKafkaConsumer(
      Consumer<KafkaKey, KafkaMessageEnvelope> consumer,
      VeniceProperties props,
      boolean isKafkaConsumerOffsetCollectionEnabled) {
    this.kafkaConsumer = consumer;
    this.consumerPollRetryTimes = props.getInt(CONSUMER_POLL_RETRY_TIMES_CONFIG, CONSUMER_POLL_RETRY_TIMES_DEFAULT);
    this.consumerPollRetryBackoffMs =
        props.getInt(CONSUMER_POLL_RETRY_BACKOFF_MS_CONFIG, CONSUMER_POLL_RETRY_BACKOFF_MS_DEFAULT);
    this.topicPartitionsOffsetsTracker =
        isKafkaConsumerOffsetCollectionEnabled ? Optional.of(new TopicPartitionsOffsetsTracker()) : Optional.empty();
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
  public void subscribe(String topic, int partition, long lastReadOffset) {
    TopicPartition topicPartition = new TopicPartition(topic, partition);

    Set<TopicPartition> topicPartitionSet = kafkaConsumer.assignment();
    if (!topicPartitionSet.contains(topicPartition)) {
      List<TopicPartition> topicPartitionList = new ArrayList<>(topicPartitionSet);
      topicPartitionList.add(topicPartition);
      kafkaConsumer.assign(topicPartitionList);
      seekNextOffset(topicPartition, lastReadOffset);
      LOGGER.info("Subscribed to Topic: {} Partition: {} Offset: {}", topic, partition, lastReadOffset);
    } else {
      LOGGER
          .warn("Already subscribed on Topic: {} Partition: {}, ignore the request of subscription.", topic, partition);
    }
  }

  @Override
  public void unSubscribe(String topic, int partition) {
    TopicPartition topicPartition = new TopicPartition(topic, partition);

    Set<TopicPartition> topicPartitionSet = kafkaConsumer.assignment();
    if (topicPartitionSet.contains(topicPartition)) {
      List<TopicPartition> topicPartitionList = new ArrayList<>(topicPartitionSet);
      if (topicPartitionList.remove(topicPartition)) {
        kafkaConsumer.assign(topicPartitionList);
      }
    }
    topicPartitionsOffsetsTracker
        .ifPresent(partitionsOffsetsTracker -> partitionsOffsetsTracker.removeTrackedOffsets(topicPartition));
  }

  @Override
  public void batchUnsubscribe(Set<TopicPartition> topicPartitionSet) {
    Set<TopicPartition> newTopicPartitionAssignment = new HashSet<>(kafkaConsumer.assignment());

    newTopicPartitionAssignment.removeAll(topicPartitionSet);
    kafkaConsumer.assign(newTopicPartitionAssignment);
  }

  @Override
  public void resetOffset(String topic, int partition) {
    if (!hasSubscription(topic, partition)) {
      throw new UnsubscribedTopicPartitionException(topic, partition);
    }
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    kafkaConsumer.seekToBeginning(Collections.singletonList(topicPartition));
  }

  @Override
  public ConsumerRecords<KafkaKey, KafkaMessageEnvelope> poll(long timeoutMs) {
    // The timeout is not respected when hitting UNKNOWN_TOPIC_OR_PARTITION and when the
    // fetcher.retrieveOffsetsByTimes call inside kafkaConsumer times out,
    // TODO: we may want to wrap this call in our own thread to enforce the timeout...
    int attemptCount = 1;
    ConsumerRecords<KafkaKey, KafkaMessageEnvelope> records = ConsumerRecords.empty();
    while (attemptCount <= consumerPollRetryTimes && !Thread.currentThread().isInterrupted()) {
      try {
        records = kafkaConsumer.poll(Duration.ofMillis(timeoutMs));
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

    if (topicPartitionsOffsetsTracker.isPresent()) {
      topicPartitionsOffsetsTracker.get().updateEndOffsets(records, kafkaConsumer.metrics());
    }
    return records;
  }

  @Override
  public boolean hasAnySubscription() {
    return !kafkaConsumer.assignment().isEmpty();
  }

  @Override
  public boolean hasSubscription(String topic, int partition) {
    TopicPartition tp = new TopicPartition(topic, partition);
    return kafkaConsumer.assignment().contains(tp);
  }

  /**
   * If the partitions were not previously subscribed, this method is a no-op.
   */
  @Override
  public void pause(String topic, int partition) {
    TopicPartition tp = new TopicPartition(topic, partition);
    if (kafkaConsumer.assignment().contains(tp)) {
      kafkaConsumer.pause(Collections.singletonList(tp));
    }
  }

  /**
   * If the partitions were not previously paused or if they were not subscribed at all, this method is a no-op.
   */
  @Override
  public void resume(String topic, int partition) {
    TopicPartition tp = new TopicPartition(topic, partition);
    if (kafkaConsumer.assignment().contains(tp)) {
      kafkaConsumer.resume(Collections.singletonList(tp));
    }
  }

  @Override
  public Set<TopicPartition> getAssignment() {
    return kafkaConsumer.assignment();
  }

  @Override
  public void close() {
    topicPartitionsOffsetsTracker.ifPresent(offsetsTracker -> offsetsTracker.clearAllOffsetState());
    if (kafkaConsumer != null) {
      try {
        kafkaConsumer.close(Duration.ZERO);
      } catch (Exception e) {
        LOGGER.warn("{} threw an exception while closing.", kafkaConsumer.getClass().getSimpleName(), e);
      }
    }
  }

  @Override
  public long getOffsetLag(String topic, int partition) {
    return topicPartitionsOffsetsTracker.isPresent()
        ? topicPartitionsOffsetsTracker.get().getOffsetLag(topic, partition)
        : -1;
  }

  @Override
  public long getLatestOffset(String topic, int partition) {
    return topicPartitionsOffsetsTracker.isPresent()
        ? topicPartitionsOffsetsTracker.get().getEndOffset(topic, partition)
        : -1;
  }
}
