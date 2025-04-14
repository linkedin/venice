package com.linkedin.venice.pubsub.api;

import static com.linkedin.venice.pubsub.PubSubPositionTypeRegistry.LATEST_POSITION_RESERVED_TYPE_ID;

import com.linkedin.venice.memory.ClassSizeEstimator;
import java.nio.ByteBuffer;


/**
 * Represents a special {@link PubSubPosition} indicating the latest available message in a partition.
 * <p>
 * This position acts as a symbolic marker used to initiate consumption from the most recent offset in a topic-partition.
 * All PubSub adapters are required to support this position and map it to the appropriate "latest" offset or
 * equivalent marker in the underlying PubSub system (e.g., Kafka's "latest" offset).
 * <p>
 * This class is typically treated as a singleton and has a fixed, reserved position type ID,
 * explicitly defined in code and used during serialization.
 * <p>
 */
final class LatestPosition implements PubSubPosition {
  private static final LatestPosition INSTANCE = new LatestPosition();

  private static final int SHALLOW_CLASS_OVERHEAD = ClassSizeEstimator.getClassOverhead(PubSubPosition.class);
  private static final String NAME = "LATEST";

  private LatestPosition() {
    // Private constructor for singleton
  }

  public static LatestPosition getInstance() {
    return INSTANCE;
  }

  @Override
  public int getHeapSize() {
    return SHALLOW_CLASS_OVERHEAD;
  }

  @Override
  public int comparePosition(PubSubPosition other) {
    throw new UnsupportedOperationException("Cannot compare LATEST position");
  }

  @Override
  public long diff(PubSubPosition other) {
    throw new UnsupportedOperationException("Cannot diff LATEST position");
  }

  @Override
  public PubSubPositionWireFormat getPositionWireFormat() {
    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.setType(LATEST_POSITION_RESERVED_TYPE_ID);
    wireFormat.setRawBytes(ByteBuffer.wrap(new byte[0]));
    return wireFormat;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public boolean equals(Object obj) {
    // All instances of LatestPosition are equal
    return obj instanceof LatestPosition;
  }

  @Override
  public int hashCode() {
    // Use the class hash code to ensure uniqueness and consistency
    return LatestPosition.class.hashCode();
  }

  @Override
  public long getNumericOffset() {
    return -1;
  }
}
