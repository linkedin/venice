package com.linkedin.venice.pubsub.api;

/**
 * Compares two {@link PubSubPosition} instances within the same topic-partition.
 *
 * <p>Implementations are expected to be PubSub-system-specific. For example, a Kafka implementation
 * compares numeric offsets, while an xinfra implementation may perform epoch-aware, shard-validated
 * comparison.
 *
 * <p>This interface intentionally does not require a {@link PubSubConsumerAdapter}. Position comparison
 * is a pure function of the positions themselves and does not require broker connectivity.
 *
 * <p>Symbolic positions ({@link PubSubSymbolicPosition#EARLIEST}, {@link PubSubSymbolicPosition#LATEST})
 * are not supported. Callers must resolve symbolic positions to concrete positions before invoking
 * {@link #comparePositions}.
 */
public interface PubSubPositionComparer {
  /**
   * Compares two concrete {@link PubSubPosition} instances within the same topic-partition.
   *
   * @param partition the topic-partition context for the comparison
   * @param position1 the first position to compare (must not be null or symbolic)
   * @param position2 the second position to compare (must not be null or symbolic)
   * @return a negative value if {@code position1} is before {@code position2}, zero if equal,
   *         or a positive value if {@code position1} is after {@code position2}
   * @throws IllegalArgumentException if either position is null or symbolic
   */
  long comparePositions(PubSubTopicPartition partition, PubSubPosition position1, PubSubPosition position2);
}
