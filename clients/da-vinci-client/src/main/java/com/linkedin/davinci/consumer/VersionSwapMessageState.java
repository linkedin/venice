package com.linkedin.davinci.consumer;

import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.VersionSwap;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * A class initialized to indicate that the changelog consumer is undergoing version swap. The class is also keeping
 * various states about this version swap. This class is NOT thread safe and the intended caller should at least
 * acquire the subscriptionLock.readLock().
 */
@NotThreadsafe
public class VersionSwapMessageState {
  private final String oldVersionTopic;
  private final String newVersionTopic;
  private final long versionSwapGenerationId;
  private final int totalRegionCount;
  private final Map<Integer, Set<String>> receivedVersionSwapPartitionToRegionsMap;
  private final Map<Integer, PubSubPosition> partitionToVersionSwapLowWatermarkPositionMap;
  private final Set<Integer> assignedPartitions;
  private final Set<Integer> completedPartitions;
  private CompletableFuture<Void> findNewTopicCheckpointFuture;
  private Map<Integer, VeniceChangeCoordinate> newTopicCheckpoints = new HashMap<>();

  public VersionSwapMessageState(
      VersionSwap versionSwap,
      int totalRegionCount,
      Set<PubSubTopicPartition> currentAssignment) {
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
  }

  public PubSubPosition getVersionSwapLowWatermarkPosition(String topic, int partitionId) {
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

  public void setFindNewTopicCheckpointFuture(CompletableFuture<Void> findNewTopicCheckpointFuture) {
    this.findNewTopicCheckpointFuture = findNewTopicCheckpointFuture;
  }

  public CompletableFuture<Void> getFindNewTopicCheckpointFuture() {
    return findNewTopicCheckpointFuture;
  }

  public void setNewTopicCheckpoints(Map<Integer, VeniceChangeCoordinate> newTopicCheckpoints) {
    this.newTopicCheckpoints = newTopicCheckpoints;
  }

  public Set<VeniceChangeCoordinate> getNewTopicCheckpoints() {
    // Defensive coding
    if (findNewTopicCheckpointFuture == null || !findNewTopicCheckpointFuture.isDone()) {
      throw new IllegalStateException("New topic checkpoints are not available yet");
    }
    Set<VeniceChangeCoordinate> checkpoints = new HashSet<>();
    for (Map.Entry<Integer, VeniceChangeCoordinate> entry: newTopicCheckpoints.entrySet()) {
      if (completedPartitions.contains(entry.getKey())) {
        checkpoints.add(entry.getValue());
      }
    }
    return checkpoints;
  }

  public Set<Integer> getAssignedPartitions() {
    return assignedPartitions;
  }

  /**
   * @return true if all partitions have received all the version swap messages required for this version swap event.
   * This means we can subscribe to the new version topic and resume normal consumption from the first relevant version
   * swap message.
   */
  public boolean isVersionSwapMessagesReceivedForAllPartitions() {
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
  public boolean handleVersionSwap(
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
  public void handleUnsubscribe(Set<Integer> partitions) {
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
