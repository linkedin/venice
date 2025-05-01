package com.linkedin.venice.pubsub.api;

final public class PubSubSymbolicPosition {
  /**
   * Singleton instance representing the earliest retrievable position in a partition.
   * Acts as a symbolic marker to start consuming from the beginning.
   *
   * @see EarliestPosition
   */
  public static final PubSubPosition EARLIEST = EarliestPosition.getInstance();
  /**
   * Singleton instance representing the latest retrievable position in a partition.
   * Acts as a symbolic marker to start consuming from the tail of the stream.
   *
   * @see LatestPosition
   */
  public static final PubSubPosition LATEST = LatestPosition.getInstance();
}
