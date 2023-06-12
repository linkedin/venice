package com.linkedin.venice.serialization.avro;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serializer.AvroGenericDeserializer;
import com.linkedin.venice.serializer.AvroSerializer;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.avro.Schema;


/**
 * This class is a thin wrapper of {@link AvroSerializer} and {@link AvroGenericDeserializer}.
 *
 * It is necessary to have a separate class responsible for this in this module in order to implement
 * Kafka's interface, which is a dependency we do not want to leak into the schema-common module where
 * the two above classes are located.
 */
public class VeniceAvroKafkaSerializer implements VeniceKafkaSerializer<Object> {
  private final AvroSerializer<Object> serializer;
  private final AvroGenericDeserializer<Object> deserializer;

  public VeniceAvroKafkaSerializer(String schemaStr) {
    this(AvroCompatibilityHelper.parse(schemaStr));
  }

  public VeniceAvroKafkaSerializer(Schema schema) {
    this.serializer = new AvroSerializer<>(schema);
    this.deserializer = new AvroGenericDeserializer<>(schema, schema);
  }

  /**
   * Close this serializer.
   * This method has to be idempotent if the serializer is used in KafkaProducer because it might be called
   * multiple times.
   */
  @Override
  public void close() {
    /* This function is not used, but is required for the interfaces. */
  }

  /**
   * Configure this class.
   * @param configMap configs in key/value pairs
   * @param isKey whether is for key or value
   */
  @Override
  public void configure(Map<String, ?> configMap, boolean isKey) {
    /* This function is not used, but is required for the interfaces. */
  }

  public byte[] serialize(String topic, Object object) {
    return this.serializer.serialize(object);
  }

  public Object deserialize(String topic, byte[] bytes) {
    return deserialize(bytes);
  }

  public Object deserialize(byte[] bytes) {
    return this.deserializer.deserialize(bytes);
  }

  public Object deserialize(ByteBuffer byteBuffer) {
    return this.deserializer.deserialize(byteBuffer);
  }
}
