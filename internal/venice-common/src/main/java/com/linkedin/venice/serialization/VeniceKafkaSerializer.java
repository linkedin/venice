package com.linkedin.venice.serialization;

import java.io.Closeable;
import java.util.Map;


/**
 * Map objects to byte arrays and back again
 *
 * @param <T> The type of the object that is mapped by this serializer
 */
public interface VeniceKafkaSerializer<T> extends Closeable {
  /**
   * Close this serializer.
   * This method has to be idempotent if the serializer is used in KafkaProducer because it might be called
   * multiple times.
   */
  void close();

  /**
   * Configure this class.
   * @param configMap configs in key/value pairs
   * @param isKey whether is for key or value
   */
  void configure(Map<String, ?> configMap, boolean isKey);

  /**
   * Construct an array of bytes from the given object
   * @param topic Topic to which the object belongs.
   * @param object The object
   * @return The bytes taken from the object
   */
  byte[] serialize(String topic, T object);

  /**
   * Create an object from an array of bytes
   * @param topic Topic to which the array of bytes belongs.
   * @param bytes An array of bytes with the objects data
   * @return A java object serialzed from the bytes
   */
  T deserialize(String topic, byte[] bytes);

}
