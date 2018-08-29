package com.linkedin.venice.serialization;

import java.util.Map;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;


/**
 * Default Kafka VeniceKafkaSerializer
 */
public class DefaultSerializer implements VeniceKafkaSerializer<byte[]> {
  ByteArraySerializer serializer;
  ByteArrayDeserializer deserializer;

  public DefaultSerializer() {
    this.serializer = new ByteArraySerializer();
    this.deserializer = new ByteArrayDeserializer();
  }

  @Override
  public void close() {
    /* This function is not used, but is required for the interface. */
  }

  @Override
  public void configure(Map<String, ?> configMap, boolean isKey) {
    /* This function is not used, but is required for the interfaces. */
  }

  @Override
  public byte[] serialize(String topic, byte[] bytes) {
    return serializer.serialize(topic, bytes);
  }

  @Override
  public byte[] deserialize(String topic, byte[] bytes) {
    return deserializer.deserialize(topic, bytes);
  }
}
