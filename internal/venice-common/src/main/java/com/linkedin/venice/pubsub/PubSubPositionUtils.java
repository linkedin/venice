package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.adapter.kafka.ApacheKafkaOffsetPosition;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import java.nio.ByteBuffer;


/**
 * Utility class for converting PubSubPosition to and from byte arrays
 */
public class PubSubPositionUtils {
  /**
   * Convert a PubSubPosition to a byte array
   * @param bytes the byte array representation of the position type and position value
   * @return the position object
   */
  public static PubSubPosition fromBytes(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int positionType = buffer.getInt();
    if (positionType == PubSubPositionType.APACHE_KAFKA_OFFSET) {
      return new ApacheKafkaOffsetPosition(buffer.getLong());
    }
    throw new IllegalArgumentException("Unknown position type: " + positionType);
  }

  /**
   * Convert a PubSubPosition to a byte array
   * @param position the position to convert
   * @return the byte array representation of the position type and position value
   */
  public static byte[] toBytes(PubSubPosition position) {
    if (position == null) {
      throw new IllegalArgumentException("Cannot convert null position to bytes");
    }

    // first 4 bytes are the position type (int) and the rest is the position value
    ByteBuffer buffer;
    if (position instanceof ApacheKafkaOffsetPosition) {
      buffer = ByteBuffer.allocate(12);
      buffer.putInt(PubSubPositionType.APACHE_KAFKA_OFFSET);
      buffer.putLong(((ApacheKafkaOffsetPosition) position).getOffset());
    } else {
      throw new IllegalArgumentException("Unknown position type: " + position.getClass().getName());
    }

    return buffer.array();
  }
}
