package com.linkedin.venice.unit.kafka.consumer;

import com.linkedin.venice.kafka.consumer.KafkaConsumerWrapper;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import com.linkedin.venice.unit.kafka.consumer.poll.PollStrategy;
import com.linkedin.venice.unit.kafka.consumer.poll.RandomPollStrategy;
import java.util.HashMap;
import java.util.Map;
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
  private final Map<TopicPartition, OffsetRecord> offsets = new HashMap<>();
  private final PollStrategy pollStrategy;

  public MockInMemoryConsumer(InMemoryKafkaBroker broker, PollStrategy pollStrategy) {
    this.broker = broker;
    this.pollStrategy = pollStrategy;
  }

  @Override
  public void subscribe(String topic, int partition, OffsetRecord offset) {
    offsets.put(new TopicPartition(topic, partition), offset);
  }

  @Override
  public void unSubscribe(String topic, int partition) {
    offsets.remove(new TopicPartition(topic, partition));
  }

  @Override
  public void resetOffset(String topic, int partition) {
    offsets.put(new TopicPartition(topic, partition), new OffsetRecord(OffsetRecord.LOWEST_OFFSET));
  }

  @Override
  public void close() {
    // No-op
  }

  @Override
  public ConsumerRecords poll(long timeout) {
    return pollStrategy.poll(broker, offsets, timeout);
  }
}
