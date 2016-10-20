package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;

/**
 * Serializer for the Avro-based kafka protocol defined in:
 *
 * {@link KafkaMessageEnvelope}
 */
public class KafkaValueSerializer extends InternalAvroSpecificSerializer<KafkaMessageEnvelope> {
  public KafkaValueSerializer() {
    super(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE);
  }
}
