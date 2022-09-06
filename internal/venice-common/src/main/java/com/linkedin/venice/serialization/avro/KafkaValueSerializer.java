package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;


/**
 * Serializer for the Avro-based kafka protocol defined in:
 *
 * {@link KafkaMessageEnvelope}
 *
 * This class needs to be defined explicitly, rather than just instantiating a
 * {@link InternalAvroSpecificSerializer} because it is used as a config passed to
 * the Kafka producer.
 */
public class KafkaValueSerializer extends InternalAvroSpecificSerializer<KafkaMessageEnvelope> {
  public KafkaValueSerializer() {
    super(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE);
  }
}
