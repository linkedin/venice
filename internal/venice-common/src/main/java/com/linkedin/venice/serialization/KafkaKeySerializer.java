package com.linkedin.venice.serialization;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.message.KafkaKey;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;


/**
 * {@link VeniceKafkaSerializer} to encode/decode {@link KafkaKey} for Venice customized kafka message
 * Used by Kafka to convert to/from byte arrays.
 *
 * {@link KafkaKey} Schema (in order)
 * - Key header byte - Either 0 (PUT or DELETE), or 2 (CONTROL_MESSAGE)
 * - Payload (Key Object)
 *
 */
public class KafkaKeySerializer implements VeniceKafkaSerializer<KafkaKey> {
  private static final int KEY_HEADER_OFFSET = 0;
  private static final int KEY_HEADER_SIZE = 1;
  private static final int KEY_PAYLOAD_OFFSET = KEY_HEADER_OFFSET + KEY_HEADER_SIZE;

  private static class ReusableObjects {
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
  }

  private static final ThreadLocal<ReusableObjects> threadLocalReusableObjects =
      ThreadLocal.withInitial(ReusableObjects::new);

  public KafkaKeySerializer() {
    /* This constructor is not used, but is required for compilation */
  }

  @Override
  /**
   * Converts from a byte[] to a {@link KafkaKey}
   * @param bytes - byte[] to be converted
   * @return Converted {@link KafkaKey}
   * */
  public KafkaKey deserialize(String topic, byte[] bytes) {
    // If single-threaded, ByteBuffer can be re-used. TODO: explore GC tuning later.
    ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length - KEY_HEADER_SIZE);
    byteBuffer.put(bytes, KEY_PAYLOAD_OFFSET, bytes.length - KEY_HEADER_SIZE);
    return new KafkaKey(bytes[KEY_HEADER_OFFSET], byteBuffer.array());
  }

  @Override
  /**
   * Converts from a {@link KafkaKey} to a byte[]
   * @param kafkaKey - {@link KafkaKey} to be converted
   * @return Converted byte[]
   * */
  public byte[] serialize(String topic, KafkaKey kafkaKey) {
    ReusableObjects reusableObjects = threadLocalReusableObjects.get();
    ByteArrayOutputStream byteArrayOutputStream = reusableObjects.byteArrayOutputStream;
    byteArrayOutputStream.reset();

    try {
      byteArrayOutputStream.write(kafkaKey.getKeyHeaderByte());
      byteArrayOutputStream.write(kafkaKey.getKey());
    } catch (IOException e) {
      throw new VeniceException("Failed to serialize message: " + kafkaKey, e);
    }
    return byteArrayOutputStream.toByteArray();
  }

  @Override
  /**
   * Configure the serializer.
   * @param configMap Configuration for the serializer.
   * @param isKey true if the serializer is going to be used for Keys.
   * @throws VeniceException if the serializer is going to be used for non Key data.
   */
  public void configure(Map<String, ?> configMap, boolean isKey) {
    if (isKey == false) {
      throw new VeniceException("Cannot use KafkaKeySerializer for non key data.");
    }
  }

  @Override
  public void close() {
    /* This function is not used, but is required for by the Deserializer interface. */
  }
}
