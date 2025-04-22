package com.linkedin.venice.pubsub;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;


/**
 * A {@link PubSubPositionFactory} for the symbolic {@link PubSubSymbolicPosition#LATEST} marker.
 * <p>
 * This factory returns the singleton instance of the latest retrievable position.
 */
public class LatestPositionFactory extends PubSubPositionFactory {
  public LatestPositionFactory(int positionTypeId) {
    super(positionTypeId);
  }

  @Override
  public PubSubPosition fromWireFormat(PubSubPositionWireFormat positionWireFormat) {
    return PubSubSymbolicPosition.LATEST;
  }

  @Override
  public String getPubSubPositionClassName() {
    return PubSubSymbolicPosition.LATEST.getClass().getName();
  }
}
