package com.linkedin.venice.unit.kafka;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;

/**
 * A single Kafka message, strongly typed for the types that Venice uses.
 *
 * @see InMemoryKafkaTopic
 */
public class InMemoryKafkaMessage {
  public final KafkaKey key;
  public final KafkaMessageEnvelope value;
  public InMemoryKafkaMessage(KafkaKey key, KafkaMessageEnvelope value) {
    this.key = key;
    this.value = value;
  }
}
