package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.annotation.RestrictedApi;
import com.linkedin.venice.memory.Measurable;
import com.linkedin.venice.pubsub.PubSubPositionDeserializer;
import java.nio.ByteBuffer;


/**
 * Represents a position of a message in a partition of a topic.
 */
public interface PubSubPosition extends Measurable {
  @RestrictedApi("This API facilitates the transition from numeric offsets to PubSubPosition. "
      + "It should be removed once the codebase fully adopts PubSubPosition.")
  long getNumericOffset();

  boolean equals(Object obj);

  int hashCode();

  /**
   * Position wrapper is used to wrap the position type and the position value.
   * This is used to serialize and deserialize the position object when sending and receiving it over the wire.
   * @return the position wrapper
   */
  PubSubPositionWireFormat getPositionWireFormat();

  /**
   * Returns the serialized wire format bytes of this position.
   * @return byte array representing the position in wire format
   */
  default ByteBuffer getWireFormatBytes() {
    return getPositionWireFormat().getRawBytes();
  }

  static PubSubPosition getPositionFromWireFormat(PubSubPositionWireFormat positionWireFormat) {
    return PubSubPositionDeserializer.getPositionFromWireFormat(positionWireFormat);
  }
}
