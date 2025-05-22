package com.linkedin.venice.unit.kafka;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;


public class InMemoryPubSubPosition implements PubSubPosition {
  private final long internalOffset;

  public InMemoryPubSubPosition(long offset) {
    this.internalOffset = offset;
  }

  @Override
  public int comparePosition(PubSubPosition other) {
    if (!(other instanceof InMemoryPubSubPosition)) {
      throw new IllegalArgumentException(
          "InMemoryPubSubPosition can only be compared with another InMemoryPubSubPosition");
    }
    InMemoryPubSubPosition otherPosition = (InMemoryPubSubPosition) other;
    return Long.compare(this.internalOffset, otherPosition.internalOffset);
  }

  @Override
  public long diff(PubSubPosition other) {
    if (!(other instanceof InMemoryPubSubPosition)) {
      throw new IllegalArgumentException(
          "InMemoryPubSubPosition can only be compared with another InMemoryPubSubPosition");
    }
    InMemoryPubSubPosition otherPosition = (InMemoryPubSubPosition) other;
    return this.internalOffset - otherPosition.internalOffset;
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
