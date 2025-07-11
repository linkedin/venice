package com.linkedin.venice.pubsub.mock;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessageHeaders;


/**
 * A single Kafka message, strongly typed for the types that Venice uses.
 *
 * @see InMemoryPubSubTopic
 */
public class InMemoryPubSubMessage {
  public final KafkaKey key;
  public final KafkaMessageEnvelope value;
  public final PubSubMessageHeaders headers;
  /**
   * This field indicates that whether {@link com.linkedin.venice.kafka.protocol.Put#putValue} has been changed or not.
   * Essentially, we only want to concat schema id with the actual put value once.
   */
  private boolean putValueChanged = false;

  public InMemoryPubSubMessage(KafkaKey key, KafkaMessageEnvelope value, PubSubMessageHeaders headers) {
    this.key = key;
    this.value = value;
    this.headers = headers;
  }

  public boolean isPutValueChanged() {
    return this.putValueChanged;
  }

  public void putValueChanged() {
    this.putValueChanged = true;
  }
}
