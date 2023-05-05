package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.pubsub.PubSubPositionFactory;


/**
 * Represents a position of a message in a partition of a topic.
 */
public interface PubSubPosition {
  /**
   * @param other the other position to compare to
   * @return returns 0 if the positions are equal,
   *          -1 if this position is less than the other position,
   *          and 1 if this position is greater than the other position
   */
  int comparePosition(PubSubPosition other);

  /**
   * @return the difference between this position and the other position
   */
  long diff(PubSubPosition other);

  boolean equals(Object obj);

  int hashCode();

  /**
   * Position wrapper is used to wrap the position type and the position value.
   * This is used to serialize and deserialize the position object when sending and receiving it over the wire.
   * @return the position wrapper
   */
  PubSubPositionWireFormat getPositionWireFormat();

  static PubSubPosition getPositionFromWireFormat(byte[] positionWireFormatBytes) {
    return PubSubPositionFactory.getPositionFromWireFormat(positionWireFormatBytes);
  }

  static PubSubPosition getPositionFromWireFormat(PubSubPositionWireFormat positionWireFormat) {
    return PubSubPositionFactory.getPositionFromWireFormat(positionWireFormat);
  }
}
