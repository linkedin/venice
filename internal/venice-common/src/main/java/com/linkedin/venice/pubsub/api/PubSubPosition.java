package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.pubsub.PubSubPositionFactory;


/**
 * Represents a position of a message in a partition of a topic.
 */
public interface PubSubPosition {
  /**
   * A special position representing the earliest available message in a partition. All pub-sub adapters must support
   * this position, and all pub-sub client implementations should interpret it as the earliest retrievable message in
   * the partition. Implementations must map this position to the corresponding earliest offset or equivalent marker
   * in the underlying pub-sub system.
   */
  PubSubPosition EARLIEST = new PubSubPosition() {
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
  };

  /**
   * A special position representing the latest available message in a partition. All pub-sub adapters must support
   * this position, and all pub-sub client implementations should interpret it as the most recent retrievable message
   * in the partition. Implementations must map this position to the corresponding latest offset or equivalent marker
   * in the underlying pub-sub system.
   */
  PubSubPosition LATEST = new PubSubPosition() {
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
  };

  /**
   * @param other the other position to compare to
   * @return returns 0 if the positions are equal,
   *          -1 if this position is less than the other position,
   *          and 1 if this position is greater than the other position
   */
  int comparePosition(PubSubPosition other);

  /**
   * @return the difference between this position and the other position
   */
  long diff(PubSubPosition other);

  boolean equals(Object obj);

  int hashCode();

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
