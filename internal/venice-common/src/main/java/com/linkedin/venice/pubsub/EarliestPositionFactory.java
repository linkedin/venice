package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import java.nio.ByteBuffer;


/**
 * A {@link PubSubPositionFactory} for the symbolic {@link PubSubSymbolicPosition#EARLIEST} marker.
 * <p>
 * This factory returns the singleton instance of the earliest retrievable position.
 */
public class EarliestPositionFactory extends PubSubPositionFactory {
  public EarliestPositionFactory(int positionTypeId) {
    super(positionTypeId);
  }

  @Override
  public PubSubPosition createFromByteBuffer(ByteBuffer buffer) {
    return PubSubSymbolicPosition.EARLIEST;
  }

  @Override
  public String getPubSubPositionClassName() {
    return PubSubSymbolicPosition.EARLIEST.getClass().getName();
  }
}
