package com.linkedin.venice.pubsub.mock;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import java.nio.ByteBuffer;


public class InMemoryPubSubPosition implements PubSubPosition {
  private final long internalOffset;

  private InMemoryPubSubPosition(long offset) {
    this.internalOffset = offset;
  }

  @Override
  public long getNumericOffset() {
    return internalOffset;
  }

  public static InMemoryPubSubPosition of(long offset) {
    return new InMemoryPubSubPosition(offset);
  }

  @Override
  public PubSubPositionWireFormat getPositionWireFormat() {
    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.type = -42;
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(internalOffset);
    buffer.flip();
    wireFormat.rawBytes = buffer;
    return wireFormat;
  }

  @Override
  public int getHeapSize() {
    return 0;
  }

  /**
   * Only {@link InMemoryPubSubPosition} supports this kind of API
   * @return a new {@link InMemoryPubSubPosition} with the internal offset incremented by 1
   */
  public InMemoryPubSubPosition getNextPosition() {
    return InMemoryPubSubPosition.of(internalOffset + 1);
  }

  public InMemoryPubSubPosition getPreviousPosition() {
    if (internalOffset < -1) {
      throw new IllegalStateException("Cannot get previous position for offset: " + internalOffset);
    }
    return InMemoryPubSubPosition.of(internalOffset - 1);
  }

  /**
   * Get position that is after the current position by n records.
   * @param n the number of records to skip
   * @return a new {@link InMemoryPubSubPosition} with the internal offset incremented by n
   */
  public InMemoryPubSubPosition getPositionAfterNRecords(long n) {
    return InMemoryPubSubPosition.of(internalOffset + n);
  }

  @Override
  public String toString() {
    return "InMemoryPubSubPosition{" + internalOffset + '}';
  }

  // implementation of equals and hashCode is not needed for this class
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InMemoryPubSubPosition that = (InMemoryPubSubPosition) o;
    return internalOffset == that.internalOffset;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(internalOffset);
  }
}
