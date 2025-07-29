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
   * When resolved, this maps to the end position of the topic partition, which is defined
   * as the offset of the last record in the topic plus one (i.e., the offset that would be
   * assigned to the next produced record).
   *
   * @see LatestPosition
   */
  public static final PubSubPosition LATEST = LatestPosition.getInstance();
}
