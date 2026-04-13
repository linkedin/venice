package com.linkedin.venice.spark.consistency;

import com.linkedin.venice.pubsub.api.PubSubConsumerAdapter;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;


/**
 * Wraps a {@link PubSubPosition} with a {@link PubSubConsumerAdapter} and {@link PubSubTopicPartition} needed
 * to compare it, exposing a {@link Comparable} interface. This allows the lily-pad algorithm in
 * {@link LilyPadUtils} and {@link com.linkedin.venice.utils.consistency.DiffValidationUtils} to remain
 * generic and free of pub-sub infrastructure dependencies.
 */
public class ComparablePubSubPosition implements Comparable<ComparablePubSubPosition> {
  private final PubSubPosition position;
  private final PubSubConsumerAdapter consumer;
  private final PubSubTopicPartition partition;

  public ComparablePubSubPosition(
      PubSubPosition position,
      PubSubConsumerAdapter consumer,
      PubSubTopicPartition partition) {
    this.position = position;
    this.consumer = consumer;
    this.partition = partition;
  }

  public PubSubPosition getPosition() {
    return position;
  }

  @Override
  public int compareTo(ComparablePubSubPosition other) {
    return Long.signum(consumer.comparePositions(partition, this.position, other.position));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ComparablePubSubPosition)) {
      return false;
    }
    return position.equals(((ComparablePubSubPosition) o).position);
  }

  @Override
  public int hashCode() {
    return position.hashCode();
  }

  @Override
  public String toString() {
    return position.toString();
  }
}
