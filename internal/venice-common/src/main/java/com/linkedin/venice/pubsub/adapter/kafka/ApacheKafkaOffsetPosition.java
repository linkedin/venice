package com.linkedin.venice.pubsub.adapter.kafka;

import com.linkedin.venice.pubsub.PubSubPositionUtils;
import com.linkedin.venice.pubsub.api.PubSubPosition;


/**
 * Offset position for Apache Kafka topics
 */
public class ApacheKafkaOffsetPosition implements PubSubPosition {
  private final long offset;

  public ApacheKafkaOffsetPosition(long offset) {
    this.offset = offset;
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

  @Override
  public byte[] toBytes() {
    return PubSubPositionUtils.toBytes(this);
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
}
