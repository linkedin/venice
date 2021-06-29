package com.linkedin.venice.unit.kafka.consumer;

import com.linkedin.venice.exceptions.UnsubscribedTopicPartitionException;
import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.consumer.poll.PollStrategy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

/**
 * A {@link KafkaConsumerWrapper} implementation which reads messages from the {@link InMemoryKafkaBroker}.
 *
 * Used in unit tests as a lightweight alternative to a full-fledged integration test. Can be configured
 * with various {@link PollStrategy} implementations in order to tweak the consuming behavior.
 */
public class MockInMemoryConsumer implements KafkaConsumerWrapper {
  private final InMemoryKafkaBroker broker;
  private final Map<TopicPartition, Long> offsets = new HashMap<>();
  private final PollStrategy pollStrategy;
  private final KafkaConsumerWrapper delegate;

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
  public void subscribe(String topic, int partition, long lastReadOffset) {
    delegate.subscribe(topic, partition, lastReadOffset);
    offsets.put(new TopicPartition(topic, partition), lastReadOffset);
  }

  @Override
  public void unSubscribe(String topic, int partition) {
    delegate.unSubscribe(topic, partition);
    offsets.remove(new TopicPartition(topic, partition));
  }

  @Override
  public void batchUnsubscribe(String topic, List<Integer> partitionList) {
    delegate.batchUnsubscribe(topic, partitionList);
    for (int partition : partitionList) {
      offsets.remove(new TopicPartition(topic, partition));
    }
  }

  @Override
  public void resetOffset(String topic, int partition) {
    if (!hasSubscription(topic, partition)) {
      throw new UnsubscribedTopicPartitionException(topic, partition);
    }
    delegate.resetOffset(topic, partition);
    offsets.put(new TopicPartition(topic, partition), OffsetRecord.LOWEST_OFFSET);
  }

  @Override
  public void close() {
    delegate.close();
    // No-op
  }

  @Override
  public ConsumerRecords poll(long timeout) {
    if (null != delegate.poll(timeout)) {
      throw new IllegalArgumentException(
          "The MockInMemoryConsumer's delegate can only be used to verify calls, not to return arbitrary instances.");
    }
    return pollStrategy.poll(broker, offsets, timeout);
  }

  @Override
  public boolean hasSubscription() {
    return !offsets.isEmpty();
  }

  @Override
  public boolean hasSubscription(String topic, int partition) {
    return offsets.containsKey(new TopicPartition(topic, partition));
  }

  @Override
  public Map<TopicPartition, Long> beginningOffsets(List<TopicPartition> topicPartitions) {
    return delegate.beginningOffsets(topicPartitions);
  }

  @Override
  public Map<TopicPartition, Long> endOffsets(List<TopicPartition> topicPartitions) {
    return delegate.endOffsets(topicPartitions);
  }

  @Override
  public void assign(List<TopicPartition> topicPartitions) {
    delegate.assign(topicPartitions);
  }

  @Override
  public void seek(TopicPartition topicPartition, long nextOffset) {
    delegate.seek(topicPartition, nextOffset);
  }

  public Map<TopicPartition, Long> getOffsets() {
    return offsets;
  }

  @Override
  public void pause(String topic, int partition) {
    delegate.pause(topic, partition);
  }

  @Override
  public void resume(String topic, int partition) {
    delegate.resume(topic, partition);
  }

  @Override
  public Set<TopicPartition> paused() {
    return delegate.paused();
  }

  @Override
  public Set<TopicPartition> getAssignment() {
    return delegate.getAssignment();
  }
}
