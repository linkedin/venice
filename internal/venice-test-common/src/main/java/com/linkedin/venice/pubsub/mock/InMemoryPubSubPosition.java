package com.linkedin.venice.pubsub.mock;

import com.linkedin.venice.pubsub.PubSubPositionFactory;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionWireFormat;
import com.linkedin.venice.utils.ByteUtils;
import java.nio.ByteBuffer;


public class InMemoryPubSubPosition implements PubSubPosition {
  public static final int INMEMORY_PUBSUB_POSITION_TYPE_ID = -42;

  private final long internalOffset;
  private final boolean numericOffsetSupported;

  private InMemoryPubSubPosition(long offset) {
    this(offset, true);
  }

  private InMemoryPubSubPosition(long offset, boolean numericOffsetSupported) {
    this.internalOffset = offset;
    this.numericOffsetSupported = numericOffsetSupported;
  }

  @Override
  public long getNumericOffset() {
    if (!numericOffsetSupported) {
      throw new UnsupportedOperationException(
          "Numeric offset not supported for this position (simulating a pub sub system that does not support numeric offsets)");
    }
    return internalOffset;
  }

  /**
   * Returns the internal offset used by this implementation.
   * <p>
   * This method was added to support test cases that rely on accessing the internal offset directly,
   * without depending on {@link #getNumericOffset()}, which will be removed from the interface in the future.
   *
   * @return the internal offset value
   */
  public long getInternalOffset() {
    return internalOffset;
  }

  public static InMemoryPubSubPosition of(long offset) {
    return new InMemoryPubSubPosition(offset);
  }

  /**
   * Creates a position that simulates a pub sub system whose positions do not support numeric offsets.
   * The internal offset is still used for mock broker indexing, but {@link #getNumericOffset()}
   * throws {@link UnsupportedOperationException}.
   */
  public static InMemoryPubSubPosition ofNonNumeric(long offset) {
    return new InMemoryPubSubPosition(offset, false);
  }

  public static InMemoryPubSubPosition of(ByteBuffer buffer) {
    if (buffer == null || buffer.limit() < Long.BYTES) {
      throw new IllegalArgumentException("Buffer must contain at least " + Long.BYTES + " bytes");
    }
    return of(ByteUtils.readLong(ByteUtils.extractByteArray(buffer), 0)); // peek without advancing
  }

  @Override
  public PubSubPositionWireFormat getPositionWireFormat() {
    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.type = INMEMORY_PUBSUB_POSITION_TYPE_ID;
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(internalOffset);
    buffer.flip();
    wireFormat.rawBytes = buffer;
    return wireFormat;
  }

  @Override
  public Class<? extends PubSubPositionFactory> getFactoryClass() {
    return InMemoryPubSubPositionFactory.class;
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
    return new InMemoryPubSubPosition(internalOffset + 1, numericOffsetSupported);
  }

  public InMemoryPubSubPosition getPreviousPosition() {
    if (internalOffset < -1) {
      throw new IllegalStateException("Cannot get previous position for offset: " + internalOffset);
    }
    return new InMemoryPubSubPosition(internalOffset - 1, numericOffsetSupported);
  }

  /**
   * Get position that is after the current position by n records.
   * @param n the number of records to skip
   * @return a new {@link InMemoryPubSubPosition} with the internal offset incremented by n
   */
  public InMemoryPubSubPosition getPositionAfterNRecords(long n) {
    return new InMemoryPubSubPosition(internalOffset + n, numericOffsetSupported);
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
