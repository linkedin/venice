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
  /**
   * This field indicates that whether {@link com.linkedin.venice.kafka.protocol.Put#putValue} has been changed or not.
   * Essentially, we only want to concat schema id with the actual put value once.
   */
  private boolean putValueChanged = false;

  public InMemoryKafkaMessage(KafkaKey key, KafkaMessageEnvelope value) {
    this.key = key;
    this.value = value;
  }

  public boolean isPutValueChanged() {
    return this.putValueChanged;
  }

  public void putValueChanged() {
    this.putValueChanged = true;
  }
}
