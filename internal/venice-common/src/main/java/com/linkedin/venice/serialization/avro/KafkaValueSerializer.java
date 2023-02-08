package com.linkedin.venice.serialization.avro;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import java.util.function.BiConsumer;
import org.apache.avro.Schema;


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

  public KafkaValueSerializer(BiConsumer<Integer, Schema> newSchemaEncountered) {
    super(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE, null, newSchemaEncountered);
  }
}
