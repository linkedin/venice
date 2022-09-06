package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;


/**
 * This interface is used inside of the {@link com.linkedin.venice.unit.kafka.consumer.MockInMemoryConsumer}
 * in order to mess around with the way messages are delivered to the consuming code.
 *
 * This is used in unit tests in order to control message deliver order, introduce duplicates, inject new
 * arbitrary messages, skip messages...
 */
public interface PollStrategy {
  ConsumerRecords poll(InMemoryKafkaBroker broker, Map<TopicPartition, Long> offsets, long timeout);
}
