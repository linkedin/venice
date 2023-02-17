package com.linkedin.venice.unit.kafka.consumer.poll;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubMessages;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.unit.kafka.InMemoryKafkaBroker;
import java.util.Map;


/**
 * This interface is used inside of the {@link com.linkedin.venice.unit.kafka.consumer.MockInMemoryConsumer}
 * in order to mess around with the way messages are delivered to the consuming code.
 *
 * This is used in unit tests in order to control message deliver order, introduce duplicates, inject new
 * arbitrary messages, skip messages...
 */
public interface PollStrategy {
  PubSubMessages<KafkaKey, KafkaMessageEnvelope, Long> poll(
      InMemoryKafkaBroker broker,
      Map<PubSubTopicPartition, Long> offsets,
      long timeout);
}
