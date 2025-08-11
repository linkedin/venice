package com.linkedin.venice.pubsub.api;

import static com.linkedin.venice.pubsub.PubSubPositionTypeRegistry.LATEST_POSITION_RESERVED_TYPE_ID;

import com.linkedin.venice.memory.ClassSizeEstimator;
import com.linkedin.venice.pubsub.LatestPositionFactory;
import com.linkedin.venice.pubsub.PubSubPositionFactory;
import java.nio.ByteBuffer;


/**
 * Represents a special {@link PubSubPosition} that indicates the latest available position in a partition.
 *
 * <p>This symbolic marker is used to initiate consumption from the end of a topic-partition,
 * meaning the offset that would be assigned to the next message (i.e., one past the last existing message).</p>
 *
 * <p>All PubSub adapters are required to support this position and resolve it to the appropriate
 * end position in the underlying PubSub system (e.g., Kafka's endOffset or equivalent).</p>
 *
 * <p>This class is treated as a singleton and has a fixed, reserved position type ID.
 * This ID is explicitly defined in code and used during serialization and deserialization.</p>
 */
final class LatestPosition implements PubSubPosition {
  private static final LatestPosition INSTANCE = new LatestPosition();

  private static final int SHALLOW_CLASS_OVERHEAD = ClassSizeEstimator.getClassOverhead(PubSubPosition.class);
  private static final String NAME = "LATEST";

  private LatestPosition() {
    // Private constructor for singleton
  }

  static LatestPosition getInstance() {
    return INSTANCE;
  }

  @Override
  public boolean isSymbolic() {
    // LatestPosition is a symbolic position
    return true;
  }

  @Override
  public int getHeapSize() {
    return SHALLOW_CLASS_OVERHEAD;
  }

  @Override
  public PubSubPositionWireFormat getPositionWireFormat() {
    PubSubPositionWireFormat wireFormat = new PubSubPositionWireFormat();
    wireFormat.setType(LATEST_POSITION_RESERVED_TYPE_ID);
    wireFormat.setRawBytes(ByteBuffer.wrap(new byte[0]));
    return wireFormat;
  }

  @Override
  public Class<? extends PubSubPositionFactory> getFactoryClass() {
    return LatestPositionFactory.class;
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
    return Long.MAX_VALUE;
  }
}
