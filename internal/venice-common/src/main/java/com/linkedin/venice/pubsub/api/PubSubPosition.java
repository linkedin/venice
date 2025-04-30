package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.annotation.RestrictedApi;
import com.linkedin.venice.annotation.UnderDevelopment;
import com.linkedin.venice.memory.Measurable;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;


/**
 * Represents a position of a message in a partition of a topic.
 */
public interface PubSubPosition extends Measurable {
  /**
   * @param other the other position to compare to
   * @return returns 0 if the positions are equal,
   *          -1 if this position is less than the other position,
   *          and 1 if this position is greater than the other position
   */
  @RestrictedApi("DO NOT USE THIS API for new code")
  @UnderDevelopment("Compare API is under development and may change in the future.")
  int comparePosition(PubSubPosition other);

  /**
   * @return the difference between this position and the other position
   */
  @RestrictedApi("DO NOT USE THIS API for new code")
  @UnderDevelopment("Compare API is under development and may change in the future.")
  long diff(PubSubPosition other);

  boolean equals(Object obj);

  int hashCode();

  @RestrictedApi("This API facilitates the transition from numeric offsets to PubSubPosition. "
      + "It should be removed once the codebase fully adopts PubSubPosition.")
  long getNumericOffset();

  /**
   * Position wrapper is used to wrap the position type and the position value.
   * This is used to serialize and deserialize the position object when sending and receiving it over the wire.
   * @return the position wrapper
   */
  PubSubPositionWireFormat getPositionWireFormat();

  static PubSubPosition getPositionFromWireFormat(PubSubPositionWireFormat positionWireFormat) {
    return PubSubPositionDeserializer.getPositionFromWireFormat(positionWireFormat);
  }
}
