package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.annotation.RestrictedApi;
import com.linkedin.venice.annotation.UnderDevelopment;
import com.linkedin.venice.memory.ClassSizeEstimator;
import com.linkedin.venice.memory.Measurable;
import com.linkedin.venice.pubsub.PubSubPositionFactory;


/**
 * Represents a position of a message in a partition of a topic.
 */
public interface PubSubPosition extends Measurable {
  /**
   * A special position representing the earliest available message in a partition. All pub-sub adapters must support
   * this position, and all pub-sub client implementations should interpret it as the earliest retrievable message in
   * the partition. Implementations must map this position to the corresponding earliest offset or equivalent marker
   * in the underlying pub-sub system.
   */
  PubSubPosition EARLIEST = new PubSubPosition() {
    private final int SHALLOW_CLASS_OVERHEAD = ClassSizeEstimator.getClassOverhead(PubSubPosition.class);

    @Override
    public int getHeapSize() {
      return SHALLOW_CLASS_OVERHEAD;
    }

    @Override
    public int comparePosition(PubSubPosition other) {
      throw new IllegalStateException("Cannot compare EARLIEST position");
    }

    @Override
    public long diff(PubSubPosition other) {
      throw new IllegalStateException("Cannot diff EARLIEST position");
    }

    @Override
    public PubSubPositionWireFormat getPositionWireFormat() {
      throw new IllegalStateException("Cannot serialize EARLIEST position");
    }

    @Override
    public String toString() {
      return "EARLIEST";
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this;
    }

    @Override
    public int hashCode() {
      return -1;
    }

    @Override
    public long getNumericOffset() {
      throw new IllegalStateException("Cannot get numeric offset for EARLIEST position");
    }
  };

  /**
   * A special position representing the latest available message in a partition. All pub-sub adapters must support
   * this position, and all pub-sub client implementations should interpret it as the most recent retrievable message
   * in the partition. Implementations must map this position to the corresponding latest offset or equivalent marker
   * in the underlying pub-sub system.
   */
  PubSubPosition LATEST = new PubSubPosition() {
    private final int SHALLOW_CLASS_OVERHEAD = ClassSizeEstimator.getClassOverhead(PubSubPosition.class);

    @Override
    public int getHeapSize() {
      return SHALLOW_CLASS_OVERHEAD;
    }

    @Override
    public int comparePosition(PubSubPosition other) {
      throw new IllegalStateException("Cannot compare LATEST position");
    }

    @Override
    public long diff(PubSubPosition other) {
      throw new IllegalStateException("Cannot diff LATEST position");
    }

    @Override
    public PubSubPositionWireFormat getPositionWireFormat() {
      throw new IllegalStateException("Cannot serialize LATEST position");
    }

    @Override
    public String toString() {
      return "LATEST";
    }

    @Override
    public boolean equals(Object obj) {
      return obj == this;
    }

    @Override
    public int hashCode() {
      return -2;
    }

    @Override
    public long getNumericOffset() {
      throw new IllegalStateException("Cannot get numeric offset for LATEST position");
    }
  };

  /**
   * @param other the other position to compare to
   * @return returns 0 if the positions are equal,
   *          -1 if this position is less than the other position,
   *          and 1 if this position is greater than the other position
   */
  @RestrictedApi("DO NOT USE THIS API for new code")
  @UnderDevelopment("Compare API is under development and may change in the future.")
  int comparePosition(PubSubPosition other);

  /**
   * @return the difference between this position and the other position
   */
  @RestrictedApi("DO NOT USE THIS API for new code")
  @UnderDevelopment("Compare API is under development and may change in the future.")
  long diff(PubSubPosition other);

  boolean equals(Object obj);

  int hashCode();

  @RestrictedApi("This API facilitates the transition from numeric offsets to PubSubPosition. "
      + "It should be removed once the codebase fully adopts PubSubPosition.")
  long getNumericOffset();

  /**
   * Position wrapper is used to wrap the position type and the position value.
   * This is used to serialize and deserialize the position object when sending and receiving it over the wire.
   * @return the position wrapper
   */
  PubSubPositionWireFormat getPositionWireFormat();

  static PubSubPosition getPositionFromWireFormat(byte[] positionWireFormatBytes) {
    return PubSubPositionFactory.getPositionFromWireFormat(positionWireFormatBytes);
  }

  static PubSubPosition getPositionFromWireFormat(PubSubPositionWireFormat positionWireFormat) {
    return PubSubPositionFactory.getPositionFromWireFormat(positionWireFormat);
  }
}
