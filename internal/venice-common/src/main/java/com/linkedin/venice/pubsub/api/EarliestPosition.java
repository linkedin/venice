package com.linkedin.venice.pubsub.api;

import static com.linkedin.venice.pubsub.PubSubPositionTypeRegistry.EARLIEST_POSITION_RESERVED_TYPE_ID;

import com.linkedin.venice.memory.ClassSizeEstimator;
import java.nio.ByteBuffer;


/**
 * Represents a special {@link PubSubPosition} indicating the earliest retrievable message in a partition.
 * <p>
 * This position acts as a symbolic marker used to initiate consumption from the very beginning of a topic-partition.
 * All PubSub adapters are required to support this position and map it to the appropriate "earliest" offset or
 * equivalent marker in the underlying PubSub system (e.g., offset 0 in Kafka).
 * <p>
 * This class is typically treated as a singleton and has a fixed, reserved position type ID
 * that is explicitly defined in code and used when serializing this position.
 * <p>
 */
final class EarliestPosition implements PubSubPosition {
  private static final EarliestPosition INSTANCE = new EarliestPosition();

  private static final int SHALLOW_CLASS_OVERHEAD = ClassSizeEstimator.getClassOverhead(PubSubPosition.class);
  private static final String NAME = "EARLIEST";

  private EarliestPosition() {
    // private constructor to prevent instantiation
  }

  static EarliestPosition getInstance() {
    return INSTANCE;
  }

  @Override
  public int getHeapSize() {
    return SHALLOW_CLASS_OVERHEAD;
  }

  @Override
  public int comparePosition(PubSubPosition other) {
    throw new UnsupportedOperationException("Cannot compare EARLIEST position");
  }

  @Override
  public long diff(PubSubPosition other) {
    throw new UnsupportedOperationException("Cannot diff EARLIEST position");
  }

  @Override
  public PubSubPositionWireFormat getPositionWireFormat() {
    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.setType(EARLIEST_POSITION_RESERVED_TYPE_ID);
    wireFormat.setRawBytes(ByteBuffer.wrap(new byte[0]));
    return wireFormat;
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Override
  public boolean equals(Object obj) {
    // All instances of EarliestPosition are considered equal
    return obj instanceof EarliestPosition;
  }

  @Override
  public int hashCode() {
    // Use the class hash code to ensure uniqueness and consistency
    return EarliestPosition.class.hashCode();
  }

  @Override
  public long getNumericOffset() {
    return -1;
  }
}
