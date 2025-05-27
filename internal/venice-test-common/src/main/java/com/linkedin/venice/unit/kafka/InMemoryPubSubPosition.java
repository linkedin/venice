package com.linkedin.venice.unit.kafka;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;


public class InMemoryPubSubPosition implements PubSubPosition {
  private final long internalOffset;

  public InMemoryPubSubPosition(long offset) {
    this.internalOffset = offset;
  }

  @Override
  public long getNumericOffset() {
    return internalOffset;
  }

  @Override
  public PubSubPositionWireFormat getPositionWireFormat() {
    throw new UnsupportedOperationException("getPositionWireFormat is not supported in InMemoryPubSubPosition");
  }

  @Override
  public int getHeapSize() {
    return 0;
  }
}
