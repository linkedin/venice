package com.linkedin.venice.unit.kafka.consumer;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.consumer.poll.PollStrategy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;


/**
 * A {@link KafkaConsumerWrapper} implementation which reads messages from the {@link InMemoryKafkaBroker}.
 *
 * Used in unit tests as a lightweight alternative to a full-fledged integration test. Can be configured
 * with various {@link PollStrategy} implementations in order to tweak the consuming behavior.
 *
 * When {@link MockInMemoryConsumer} is used to simulate the shared consumer behavior, there might be 2 different threads calling the methods
 * from this class. For example, consumer task thread from {@link com.linkedin.davinci.kafka.consumer.KafkaConsumerService} will
 * periodically call {@link MockInMemoryConsumer#poll(long)} and {@link com.linkedin.davinci.kafka.consumer.StoreIngestionTask} thread
 * is calling {@link MockInMemoryConsumer#resetOffset(String, int)}, which may cause test failure.
 *
 * TODO: Remove synchronized keyword in this class when consumer operations in consumption task is event-driven.
 */
public class MockInMemoryConsumer implements KafkaConsumerWrapper {
  private final InMemoryKafkaBroker broker;
  private final Map<TopicPartition, Long> offsets = new HashMap<>();
  private final PollStrategy pollStrategy;
  private final KafkaConsumerWrapper delegate;
  private final Set<TopicPartition> pausedTopicPartitions = new HashSet<>();

  /**
   * @param delegate Can be used to pass a mock, in order to verify calls. Note: functions that return
   *                 do not return the result of the mock, but rather the results of the in-memory
   *                 consumer components.
   */
  public MockInMemoryConsumer(InMemoryKafkaBroker broker, PollStrategy pollStrategy, KafkaConsumerWrapper delegate) {
    this.broker = broker;
    this.pollStrategy = pollStrategy;
    this.delegate = delegate;
  }

  @Override
  public synchronized void subscribe(String topic, int partition, long lastReadOffset) {
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    pausedTopicPartitions.remove(topicPartition);
    delegate.subscribe(topic, partition, lastReadOffset);
    offsets.put(topicPartition, lastReadOffset);
  }

  @Override
  public synchronized void unSubscribe(String topic, int partition) {
    delegate.unSubscribe(topic, partition);
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    offsets.remove(topicPartition);
    pausedTopicPartitions.remove(topicPartition);
  }

  @Override
  public void batchUnsubscribe(Set<TopicPartition> topicPartitionSet) {
    delegate.batchUnsubscribe(topicPartitionSet);
    for (TopicPartition topicPartition: topicPartitionSet) {
      offsets.remove(topicPartition);
      pausedTopicPartitions.remove(topicPartition);
    }
  }

  @Override
  public synchronized void resetOffset(String topic, int partition) {
    if (!hasSubscription(topic, partition)) {
      throw new UnsubscribedTopicPartitionException(topic, partition);
    }
    delegate.resetOffset(topic, partition);
    offsets.put(new TopicPartition(topic, partition), OffsetRecord.LOWEST_OFFSET);
  }

  @Override
  public void close() {
    delegate.close();
    pausedTopicPartitions.clear();
    offsets.clear();
  }

  @Override
  public synchronized ConsumerRecords<byte[], byte[]> poll(long timeout) {
    if (delegate.poll(timeout) != null) {
      throw new IllegalArgumentException(
          "The MockInMemoryConsumer's delegate can only be used to verify calls, not to return arbitrary instances.");
    }

    Map<TopicPartition, Long> offsetsToPoll = new HashMap<>();
    for (Map.Entry<TopicPartition, Long> entry: offsets.entrySet()) {
      TopicPartition topicPartition = entry.getKey();
      Long offset = entry.getValue();
      if (!pausedTopicPartitions.contains(entry.getKey())) {
        offsetsToPoll.put(topicPartition, offset);
      }
    }

    ConsumerRecords<byte[], byte[]> consumerRecords = pollStrategy.poll(broker, offsetsToPoll, timeout);
    for (Map.Entry<TopicPartition, Long> entry: offsetsToPoll.entrySet()) {
      TopicPartition topicPartition = entry.getKey();
      Long offsetToPoll = entry.getValue();
      if (offsets.containsKey(topicPartition)) {
        offsets.put(topicPartition, offsetToPoll);
      }
    }
    return consumerRecords;
  }

  @Override
  public boolean hasAnySubscription() {
    return !offsets.isEmpty();
  }

  @Override
  public boolean hasSubscription(String topic, int partition) {
    return offsets.containsKey(new TopicPartition(topic, partition));
  }

  public Map<TopicPartition, Long> getOffsets() {
    return offsets;
  }

  @Override
  public synchronized void pause(String topic, int partition) {
    pausedTopicPartitions.add(new TopicPartition(topic, partition));
    delegate.pause(topic, partition);
  }

  @Override
  public synchronized void resume(String topic, int partition) {
    TopicPartition topicPartitionToResume = new TopicPartition(topic, partition);
    if (pausedTopicPartitions.contains(topicPartitionToResume)) {
      pausedTopicPartitions.remove(topicPartitionToResume);
    }
    delegate.resume(topic, partition);
  }

  @Override
  public Set<TopicPartition> getAssignment() {
    return offsets.keySet();
  }
}
