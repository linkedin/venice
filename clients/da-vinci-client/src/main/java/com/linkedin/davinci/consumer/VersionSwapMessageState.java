package com.linkedin.davinci.consumer;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * A class initialized to indicate that the changelog consumer is undergoing version swap. The class is also keeping
 * various states about this version swap. This class is thread safe by having all methods that change or read a state
 * that's mutable synchronized. However, it's important to keep in mind that race can still occur if the caller is
 * trying to perform a sequence of events that depend on each other. In those scenarios an external lock is required.
 * E.g. (1) getFindNewTopicCheckpointFuture() and if the future is complete call (2) getNewTopicVersionSwapCheckpoints()
 * By the time (2) is called it's possible that the result from (1) is no longer valid. A different thread slipped in
 * between and changed the state.
 */
public class VersionSwapMessageState {
  private final String oldVersionTopic;
  private final String newVersionTopic;
  private final long versionSwapGenerationId;
  private final int totalRegionCount;
  private final Map<Integer, Set<String>> receivedVersionSwapPartitionToRegionsMap;
  private final Map<Integer, PubSubPosition> partitionToVersionSwapLowWatermarkPositionMap;
  private final Set<Integer> assignedPartitions;
  private final Set<Integer> completedPartitions;
  private final long versionSwapStartTimestamp;
  private CompletableFuture<Void> findNewTopicCheckpointFuture;
  private Map<Integer, VeniceChangeCoordinate> newTopicVersionSwapCheckpoints = new HashMap<>();
  private Map<Integer, VeniceChangeCoordinate> newTopicEOPCheckpoints = new HashMap<>();

  public VersionSwapMessageState(
      VersionSwap versionSwap,
      int totalRegionCount,
      Set<PubSubTopicPartition> currentAssignment,
      long versionSwapStartTimestamp) {
    this.oldVersionTopic = versionSwap.getOldServingVersionTopic().toString();
    this.newVersionTopic = versionSwap.getNewServingVersionTopic().toString();
    this.versionSwapGenerationId = versionSwap.getGenerationId();
    this.totalRegionCount = totalRegionCount;
    this.receivedVersionSwapPartitionToRegionsMap = new HashMap<>();
    this.assignedPartitions = new HashSet<>();
    for (PubSubTopicPartition pubSubTopicPartition: currentAssignment) {
      if (oldVersionTopic.equals(pubSubTopicPartition.getTopicName())) {
        receivedVersionSwapPartitionToRegionsMap.put(pubSubTopicPartition.getPartitionNumber(), new HashSet<>());
        assignedPartitions.add(pubSubTopicPartition.getPartitionNumber());
      }
    }
    this.partitionToVersionSwapLowWatermarkPositionMap = new HashMap<>();
    this.completedPartitions = new HashSet<>();
    this.versionSwapStartTimestamp = versionSwapStartTimestamp;
  }

  /**
   * Get the pub sub position of the first relevant version swap message for the given partition. Null will be returned
   * if the partition have not consumed its version swap yet. This is acceptable because different partitions could be
   * making progress towards version swap at different pace. e.g. partition 0 consumed its version swap message already
   * but partition 1 could still be consuming regular messages from the old version topic before encountering any
   * version swap messages.
   * @param topic where the version swap message originated from
   * @param partitionId of the version topic
   * @return the pub sub position or null
   */
  public synchronized PubSubPosition getVersionSwapLowWatermarkPosition(String topic, int partitionId) {
    if (oldVersionTopic.equals(topic)) {
      return partitionToVersionSwapLowWatermarkPositionMap.get(partitionId);
    } else {
      return null;
    }
  }

  public String getOldVersionTopic() {
    return oldVersionTopic;
  }

  public String getNewVersionTopic() {
    return newVersionTopic;
  }

  public long getVersionSwapGenerationId() {
    return versionSwapGenerationId;
  }

  public synchronized void setFindNewTopicCheckpointFuture(CompletableFuture<Void> findNewTopicCheckpointFuture) {
    this.findNewTopicCheckpointFuture = findNewTopicCheckpointFuture;
    this.newTopicEOPCheckpoints.clear();
    this.newTopicVersionSwapCheckpoints.clear();
  }

  public synchronized CompletableFuture<Void> getFindNewTopicCheckpointFuture() {
    return findNewTopicCheckpointFuture;
  }

  public synchronized void setNewTopicVersionSwapCheckpoints(
      Map<Integer, VeniceChangeCoordinate> newTopicVersionSwapCheckpoints) {
    this.newTopicVersionSwapCheckpoints = newTopicVersionSwapCheckpoints;
  }

  public synchronized void setNewTopicEOPCheckpoints(Map<Integer, VeniceChangeCoordinate> newTopicEOPCheckpoints) {
    this.newTopicEOPCheckpoints = newTopicEOPCheckpoints;
  }

  public synchronized Set<VeniceChangeCoordinate> getNewTopicVersionSwapCheckpoints() {
    // Defensive coding
    if (findNewTopicCheckpointFuture == null || !findNewTopicCheckpointFuture.isDone()) {
      throw new VeniceException("New topic checkpoints are not available yet");
    }
    Set<VeniceChangeCoordinate> checkpoints = new HashSet<>();
    for (Map.Entry<Integer, VeniceChangeCoordinate> entry: newTopicVersionSwapCheckpoints.entrySet()) {
      if (completedPartitions.contains(entry.getKey())) {
        checkpoints.add(entry.getValue());
      }
    }
    return checkpoints;
  }

  /**
   * Intended to be used as a backup strategy if any partition still did not complete version swap within the timeout.
   * Remaining partitions will be resumed from EOP instead of first relevant version swap message in the new topic.
   */
  public synchronized Set<VeniceChangeCoordinate> getNewTopicCheckpointsWithEOPAsBackup() {
    Set<VeniceChangeCoordinate> checkpoints = getNewTopicVersionSwapCheckpoints();
    for (Integer partition: getIncompletePartitions()) {
      if (newTopicEOPCheckpoints.containsKey(partition)) {
        checkpoints.add(newTopicEOPCheckpoints.get(partition));
      } else {
        throw new VeniceException(
            String.format("EOP VeniceChangeCoordinate is missing unexpectedly for partition: %s", partition));
      }
    }
    return checkpoints;
  }

  public Set<Integer> getAssignedPartitions() {
    return Collections.unmodifiableSet(assignedPartitions);
  }

  public synchronized Set<Integer> getIncompletePartitions() {
    Set<Integer> incompletePartitions = new HashSet<>(assignedPartitions);
    incompletePartitions.removeAll(completedPartitions);
    return incompletePartitions;
  }

  /**
   * If we have reached the timeout for the version swap, we need to forcefully seek to the new topic using the EOP
   * positions for any remaining partitions as our backup plan which should cover a variety of edge cases (e.g. consumer
   * is not polling fast enough, consumer starting position was in between version swaps, a region is down, etc...) In
   * all these edge cases it's better to go to the new topic and consume a lot of duplicate messages than staying on the
   * old topic which will eventually be deleted.
   * @return the timestamp when the version swap was started.
   */
  public long getVersionSwapStartTimestamp() {
    return versionSwapStartTimestamp;
  }

  /**
   * @return true if all partitions have received all the version swap messages required for this version swap event.
   * This means we can subscribe to the new version topic and resume normal consumption from the first relevant version
   * swap message.
   */
  public synchronized boolean isVersionSwapMessagesReceivedForAllPartitions() {
    return completedPartitions.size() == receivedVersionSwapPartitionToRegionsMap.size();
  }

  /**
   * Handle the version swap message and check if all version swap messages related to this version swap event have
   * been received for the given partition.
   * @param versionSwap message that we just received.
   * @param pubSubTopicPartition where we received the version swap message.
   * @param position of the version swap message.
   * @return true if all version swap messages related to this version swap event have been received.
   */
  public synchronized boolean handleVersionSwap(
      VersionSwap versionSwap,
      PubSubTopicPartition pubSubTopicPartition,
      PubSubPosition position) {
    int partitionId = pubSubTopicPartition.getPartitionNumber();
    if (!oldVersionTopic.equals(pubSubTopicPartition.getTopicName())) {
      // In theory this can only happen if should user unsubscribed and resubscribed some partitions during a version
      // swap or there is a bug in the code. Either way this is not expected/supported.
      throw new VeniceException(
          String.format(
              "Invalid state, received version swap message from %s partition: %s to %s during an ongoing version swap from %s to %s",
              pubSubTopicPartition.getTopicName(),
              partitionId,
              versionSwap.getNewServingVersionTopic(),
              oldVersionTopic,
              newVersionTopic));
    }
    if (versionSwapGenerationId == versionSwap.getGenerationId()
        && versionSwap.getNewServingVersionTopic().toString().equals(newVersionTopic)) {
      receivedVersionSwapPartitionToRegionsMap.computeIfPresent(partitionId, (p, receivedRegions) -> {
        receivedRegions.add(versionSwap.getDestinationRegion().toString());
        if (receivedRegions.size() >= totalRegionCount) {
          completedPartitions.add(p);
        }
        return receivedRegions;
      });
      partitionToVersionSwapLowWatermarkPositionMap.computeIfAbsent(partitionId, (p) -> position);
      return completedPartitions.contains(partitionId);
    }
    return false;
  }

  /**
   * Remove unsubscribed partitions from the ongoing version swap states.
   * @param partitions to unsubscribe
   */
  public synchronized void handleUnsubscribe(Set<Integer> partitions) {
    for (Integer partition: partitions) {
      receivedVersionSwapPartitionToRegionsMap.remove(partition);
      partitionToVersionSwapLowWatermarkPositionMap.remove(partition);
      assignedPartitions.remove(partition);
      completedPartitions.remove(partition);
    }
  }

  /**
   * Determines if the version swap message is relevant or not based on the version topic that it was consumed from and
   * the version swap message content.
   * @param currentVersionTopic where the version swap message was consumed from.
   * @param clientRegion of the consumer.
   * @param versionSwap message that was consumed.
   * @return if the version swap message is relevant and should be processed.
   */
  public static boolean isVersionSwapRelevant(
      String currentVersionTopic,
      String clientRegion,
      VersionSwap versionSwap) {
    if (versionSwap.generationId == -1) {
      // Old version swap message that should be ignored
      return false;
    }
    return currentVersionTopic.equals(versionSwap.getOldServingVersionTopic().toString())
        && clientRegion.equals(versionSwap.getSourceRegion().toString())
        && Version.isVersionTopic(versionSwap.getNewServingVersionTopic().toString());
  }
}
