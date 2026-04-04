package com.linkedin.venice.spark.consistency;

import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.pubsub.manager.TopicManager;


/**
 * Wraps a {@link PubSubPosition} with the {@link TopicManager} and {@link PubSubTopicPartition} needed
 * to compare it, exposing a {@link Comparable} interface. This allows the lily-pad algorithm in
 * {@link LilyPadUtils} and {@link com.linkedin.venice.utils.consistency.DiffValidationUtils} to remain
 * generic and free of pub-sub infrastructure dependencies.
 */
public class ComparablePubSubPosition implements Comparable<ComparablePubSubPosition> {
  private final PubSubPosition position;
  private final TopicManager topicManager;
  private final PubSubTopicPartition partition;

  public ComparablePubSubPosition(PubSubPosition position, TopicManager topicManager, PubSubTopicPartition partition) {
    this.position = position;
    this.topicManager = topicManager;
    this.partition = partition;
  }

  public PubSubPosition getPosition() {
    return position;
  }

  @Override
  public int compareTo(ComparablePubSubPosition other) {
    return Long.signum(topicManager.comparePosition(partition, this.position, other.position));
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
