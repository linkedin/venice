package com.linkedin.venice.pubsub.adapter.kafka.consumer;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubPositionComparer;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;


/**
 * Kafka implementation of {@link PubSubPositionComparer}.
 *
 * <p>Compares two concrete {@link PubSubPosition} instances by their numeric offset values.
 * This is a stateless, thread-safe singleton that requires no consumer or broker connectivity.
 *
 * <p>Symbolic positions ({@link com.linkedin.venice.pubsub.api.PubSubSymbolicPosition#EARLIEST},
 * {@link com.linkedin.venice.pubsub.api.PubSubSymbolicPosition#LATEST}) are rejected with an
 * {@link IllegalArgumentException}.
 */
public class ApacheKafkaPositionComparer implements PubSubPositionComparer {
  public static final ApacheKafkaPositionComparer INSTANCE = new ApacheKafkaPositionComparer();

  @Override
  public long comparePositions(PubSubTopicPartition partition, PubSubPosition position1, PubSubPosition position2) {
    if (position1 == null || position2 == null) {
      throw new IllegalArgumentException("Positions cannot be null");
    }
    if (position1.isSymbolic() || position2.isSymbolic()) {
      throw new IllegalArgumentException(
          "Symbolic positions are not supported for comparison. "
              + "Resolve symbolic positions before calling comparePositions. " + "position1: " + position1
              + ", position2: " + position2);
    }
    return Long.compare(position1.getNumericOffset(), position2.getNumericOffset());
  }
}
