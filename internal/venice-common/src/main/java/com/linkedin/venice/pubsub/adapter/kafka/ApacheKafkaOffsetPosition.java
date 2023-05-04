package com.linkedin.venice.pubsub.adapter.kafka;

import com.linkedin.venice.pubsub.PubSubPositionType;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import java.nio.ByteBuffer;
import java.util.Objects;


/**
 * Offset position for Apache Kafka topics
 */
public class ApacheKafkaOffsetPosition implements PubSubPosition {
  private final long offset;

  public ApacheKafkaOffsetPosition(long offset) {
    this.offset = offset;
  }

  public ApacheKafkaOffsetPosition(ByteBuffer buffer) {
    // read the first 8 bytes as a long
    this(Objects.requireNonNull(buffer, "Cannot create ApacheKafkaOffsetPosition with null").getLong(0));
  }

  /**
   * @param other the other position to compare to
   * @return returns 0 if the positions are equal,
   *         -1 if this position is less than the other position,
   *          and 1 if this position is greater than the other position
   */
  @Override
  public int comparePosition(PubSubPosition other) {
    validatePositionIsComparable(other);
    ApacheKafkaOffsetPosition otherPosition = (ApacheKafkaOffsetPosition) other;
    return Long.compare(offset, otherPosition.offset);
  }

  /**
   * @return the difference between this position and the other position
   *
   * @throws IllegalArgumentException if position is null or positions are not comparable
   */
  @Override
  public long diff(PubSubPosition other) {
    validatePositionIsComparable(other);
    ApacheKafkaOffsetPosition otherPosition = (ApacheKafkaOffsetPosition) other;
    return offset - otherPosition.offset;
  }

  /**
   * Checks if the other position is comparable to this position
   */
  private void validatePositionIsComparable(PubSubPosition other) {
    if (other == null) {
      throw new IllegalArgumentException("Cannot compare ApacheKafkaOffsetPosition with null");
    }

    if (!(other instanceof ApacheKafkaOffsetPosition)) {
      throw new IllegalArgumentException("Cannot compare ApacheKafkaOffsetPosition with " + other.getClass().getName());
    }
  }

  public long getOffset() {
    return offset;
  }

  @Override
  public String toString() {
    return "ApacheKafkaOffsetPosition{" + "offset=" + offset + '}';
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    ApacheKafkaOffsetPosition that = (ApacheKafkaOffsetPosition) other;
    return offset == that.offset;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(offset);
  }

  /**
   * Position wrapper is used to wrap the position type and the position value.
   * This is used to serialize and deserialize the position object when sending and receiving it over the wire.
   *
   * @return the position wrapper
   */
  @Override
  public PubSubPositionWireFormat getPositionWireFormat() {
    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.type = PubSubPositionType.APACHE_KAFKA_OFFSET;
    wireFormat.rawBytes = (ByteBuffer) ByteBuffer.allocate(Long.BYTES).putLong(offset).flip(); // flip to avoid buffer
                                                                                               // underflow
    return wireFormat;
  }
}
