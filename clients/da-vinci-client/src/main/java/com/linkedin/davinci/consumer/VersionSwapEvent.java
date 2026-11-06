package com.linkedin.davinci.consumer;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


/**
 * Describes the topic that partitions of a {@link VeniceChangelogConsumer} are consumed from, delivered to a
 * {@link VersionSwapListener}. Either a completed version swap, which moved the partitions in
 * {@link #getNewTopicPositions()} from {@link #getPreviousTopics()} to {@link #getNewTopic()}, or a topic report for
 * partitions subscribed or seeked to {@link #getNewTopic()}. See {@link #isVersionSwap()}.
 */
public final class VersionSwapEvent {
  /**
   * Generation id reported when the swap was not driven by a version swap control message, e.g. when the consumer
   * detected the new current version through store metadata.
   */
  public static final long UNKNOWN_GENERATION_ID = -1L;

  private final Set<String> previousTopics;
  private final String newTopic;
  private final int newVersion;
  private final long generationId;
  private final Set<Integer> partitions;
  private final Map<Integer, VeniceChangeCoordinate> newTopicPositions;

  public VersionSwapEvent(
      Set<String> previousTopics,
      String newTopic,
      int newVersion,
      long generationId,
      Set<Integer> partitions,
      Map<Integer, VeniceChangeCoordinate> newTopicPositions) {
    this.previousTopics = Collections.unmodifiableSet(new HashSet<>(previousTopics));
    this.newTopic = Objects.requireNonNull(newTopic, "newTopic cannot be null");
    this.newVersion = newVersion;
    this.generationId = generationId;
    this.partitions = Collections.unmodifiableSet(new HashSet<>(partitions));
    this.newTopicPositions = Collections.unmodifiableMap(new HashMap<>(newTopicPositions));
  }

  /**
   * @return true for a completed version swap; false for a topic report of subscribed or seeked partitions, which has
   *         no previous topics, no new topic positions, and {@link #UNKNOWN_GENERATION_ID}.
   */
  public boolean isVersionSwap() {
    return !previousTopics.isEmpty();
  }

  /**
   * @return the topics the swapped partitions were consumed from before the swap. Usually a single topic; empty for a
   *         topic report.
   */
  public Set<String> getPreviousTopics() {
    return previousTopics;
  }

  /**
   * @return the topic the swapped partitions are consumed from after the swap, or the topic the reported partitions
   *         were subscribed or seeked to.
   */
  public String getNewTopic() {
    return newTopic;
  }

  /**
   * @return the store version of {@link #getNewTopic()}.
   */
  public int getNewVersion() {
    return newVersion;
  }

  /**
   * @return the generation id of the version swap control message that drove this swap, or
   *         {@link #UNKNOWN_GENERATION_ID} when the swap was detected through store metadata.
   */
  public long getGenerationId() {
    return generationId;
  }

  /**
   * @return for a version swap, every partition of this consumer that is assigned to {@link #getNewTopic()} after the
   *         swap, including partitions that were already on it before the swap. For a topic report, the reported
   *         partitions.
   */
  public Set<Integer> getPartitions() {
    return partitions;
  }

  /**
   * @return for each partition moved by this swap, the position on {@link #getNewTopic()} the consumer resumed from.
   *         Passing these coordinates to {@link VeniceChangelogConsumer#seekToCheckpoint(Set)} resumes each partition
   *         from the same position, so they can replace previous-topic checkpoints of partitions that have not
   *         returned a record from the new topic yet.
   */
  public Map<Integer, VeniceChangeCoordinate> getNewTopicPositions() {
    return newTopicPositions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof VersionSwapEvent)) {
      return false;
    }
    VersionSwapEvent that = (VersionSwapEvent) o;
    return newVersion == that.newVersion && generationId == that.generationId
        && previousTopics.equals(that.previousTopics) && newTopic.equals(that.newTopic)
        && partitions.equals(that.partitions) && newTopicPositions.equals(that.newTopicPositions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(previousTopics, newTopic, newVersion, generationId, partitions, newTopicPositions);
  }

  @Override
  public String toString() {
    return "VersionSwapEvent{previousTopics=" + previousTopics + ", newTopic=" + newTopic + ", newVersion=" + newVersion
        + ", generationId=" + generationId + ", partitions=" + partitions + ", newTopicPositions=" + newTopicPositions
        + "}";
  }
}
