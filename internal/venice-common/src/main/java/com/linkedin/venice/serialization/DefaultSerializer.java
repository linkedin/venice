package com.linkedin.venice.serialization;

import java.util.Map;


/**
 * Default Kafka VeniceKafkaSerializer
 */
public class DefaultSerializer implements VeniceKafkaSerializer<byte[]> {
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
    return bytes;
  }

  @Override
  public byte[] deserialize(String topic, byte[] bytes) {
    return bytes;
  }
}
