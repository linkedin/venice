package com.linkedin.venice.unit.kafka.consumer;

import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.consumer.poll.PollStrategy;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * A {@link KafkaConsumerWrapper} implementation which reads messages from the {@link InMemoryKafkaBroker}.
 *
 * Used in unit tests as a lightweight alternative to a full-fledged integration test. Can be configured
 * with various {@link PollStrategy} implementations in order to tweak the consuming behavior.
 */
public class MockInMemoryConsumer implements KafkaConsumerWrapper {
  private final InMemoryKafkaBroker broker;
  private final Map<TopicPartition, OffsetRecord> offsets = new HashMap<>();
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
  public void subscribe(String topic, int partition, OffsetRecord offset) {
    delegate.subscribe(topic, partition, offset);
    offsets.put(new TopicPartition(topic, partition), offset);
  }

  @Override
  public void unSubscribe(String topic, int partition) {
    delegate.unSubscribe(topic, partition);
    offsets.remove(new TopicPartition(topic, partition));
  }

  @Override
  public void resetOffset(String topic, int partition) {
    delegate.resetOffset(topic, partition);
    offsets.put(new TopicPartition(topic, partition), new OffsetRecord());
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

  public Map<TopicPartition, OffsetRecord> getOffsets() {
    return offsets;
  }

}
