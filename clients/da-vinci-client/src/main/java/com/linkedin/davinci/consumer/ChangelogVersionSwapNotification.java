package com.linkedin.davinci.consumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * Reports that some partitions of a {@link VeniceChangelogConsumer} are now consumed from {@link #getNewTopic()}.
 * Delivered to the listener registered with {@link VeniceChangelogConsumer#setVersionSwapListener}.
 */
public final class ChangelogVersionSwapNotification {
  private final String newTopic;
  private final Set<Integer> partitions;
  private final Map<Integer, VeniceChangeCoordinate> newTopicPositions;

  public ChangelogVersionSwapNotification(
      String newTopic,
      Set<Integer> partitions,
      Map<Integer, VeniceChangeCoordinate> newTopicPositions) {
    this.newTopic = Objects.requireNonNull(newTopic, "newTopic cannot be null");
    this.partitions = Collections.unmodifiableSet(new HashSet<>(partitions));
    this.newTopicPositions = Collections.unmodifiableMap(new HashMap<>(newTopicPositions));
  }

  /**
   * @return the topic the partitions are now consumed from
   */
  public String getNewTopic() {
    return newTopic;
  }

  /**
   * @return the partitions of this consumer now consumed from {@link #getNewTopic()}
   */
  public Set<Integer> getPartitions() {
    return partitions;
  }

  /**
   * @return the exact position on {@link #getNewTopic()} the consumer moved each partition to. Partitions subscribed
   *         at a symbolic position (e.g. by {@link VeniceChangelogConsumer#subscribe}) have no entry.
   */
  public Map<Integer, VeniceChangeCoordinate> getNewTopicPositions() {
    return newTopicPositions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ChangelogVersionSwapNotification)) {
      return false;
    }
    ChangelogVersionSwapNotification that = (ChangelogVersionSwapNotification) o;
    return newTopic.equals(that.newTopic) && partitions.equals(that.partitions)
        && newTopicPositions.equals(that.newTopicPositions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(newTopic, partitions, newTopicPositions);
  }

  @Override
  public String toString() {
    return "ChangelogVersionSwapNotification{newTopic=" + newTopic + ", partitions=" + partitions
        + ", newTopicPositions=" + newTopicPositions + "}";
  }
}
