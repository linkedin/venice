package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class contains some common util methods for push monitoring purpose.
 */
public class PushMonitorUtils {
  private static final Logger LOGGER = LogManager.getLogger(PushMonitorUtils.class);

  /**
   * This method checks Da Vinci client push status of all partitions from push status store and compute a final status.
   * Inside each partition, this method will compute status based on all active Da Vinci instances.
   * A Da Vinci instance sent heartbeat to controllers recently is considered active.
   */
  public static ExecutionStatusWithDetails getDaVinciPushStatusAndDetails(
      PushStatusStoreReader reader,
      String topicName,
      int partitionCount,
      Optional<String> incrementalPushVersion) {
    if (reader == null) {
      throw new VeniceException("PushStatusStoreReader is null");
    }
    LOGGER.info("Getting Da Vinci push status for topic: {}", topicName);
    boolean allMiddleStatusReceived = true;
    ExecutionStatus completeStatus = incrementalPushVersion.isPresent()
        ? ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED
        : ExecutionStatus.COMPLETED;
    ExecutionStatus middleStatus = incrementalPushVersion.isPresent()
        ? ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED
        : ExecutionStatus.END_OF_PUSH_RECEIVED;
    Optional<String> erroredReplica = Optional.empty();
    int erroredPartitionId = 0;
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    int completedPartitions = 0;
    int totalReplicaCount = 0;
    int liveReplicaCount = 0;
    Set<Integer> incompletePartition = new HashSet<>();
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      Map<CharSequence, Integer> instances = reader.getPartitionStatus(
          storeName,
          Version.parseVersionFromVersionTopicName(topicName),
          partitionId,
          incrementalPushVersion);
      boolean allInstancesCompleted = true;
      totalReplicaCount += instances.size();
      for (Map.Entry<CharSequence, Integer> entry: instances.entrySet()) {
        ExecutionStatus status = ExecutionStatus.fromInt(entry.getValue());
        boolean isInstanceAlive = reader.isInstanceAlive(storeName, entry.getKey().toString());
        if (!isInstanceAlive) {
          continue;
        }
        // We only compute status based on live instances.
        liveReplicaCount++;
        if (status == completeStatus) {
          continue;
        }
        if (status == middleStatus) {
          allInstancesCompleted = false;
          continue;
        }
        allInstancesCompleted = false;
        allMiddleStatusReceived = false;
        if (status == ExecutionStatus.ERROR) {
          erroredReplica = Optional.of(entry.getKey().toString());
          erroredPartitionId = partitionId;
          break;
        }
      }
      if (allInstancesCompleted) {
        completedPartitions++;
      } else {
        incompletePartition.add(partitionId);
      }
    }
    StringBuilder statusDetailStringBuilder = new StringBuilder();
    if (completedPartitions > 0) {
      statusDetailStringBuilder.append(completedPartitions)
          .append("/")
          .append(partitionCount)
          .append(" partitions completed in")
          .append(totalReplicaCount)
          .append(" Da Vinci replicas.");
    }
    if (erroredReplica.isPresent()) {
      statusDetailStringBuilder.append("Found a failed partition replica in Da Vinci: ")
          .append("Partition: ")
          .append(erroredPartitionId)
          .append("Replica: ")
          .append(erroredReplica)
          .append(". Live replica count: ")
          .append(liveReplicaCount)
          .append(", total replica count: ")
          .append(totalReplicaCount);
    }
    int incompleteSize = incompletePartition.size();
    if (incompleteSize > 0 && incompleteSize <= 5) {
      statusDetailStringBuilder.append(". Following partitions still not complete ")
          .append(incompletePartition)
          .append(". Live replica count: ")
          .append(liveReplicaCount)
          .append(", total replica count: ")
          .append(totalReplicaCount);
    }
    String statusDetail = statusDetailStringBuilder.toString();
    boolean noDaVinciStatusReported = totalReplicaCount == 0;
    if (completedPartitions == partitionCount) {
      return new ExecutionStatusWithDetails(completeStatus, statusDetail, noDaVinciStatusReported);
    }
    if (allMiddleStatusReceived) {
      return new ExecutionStatusWithDetails(middleStatus, statusDetail, noDaVinciStatusReported);
    }
    if (erroredReplica.isPresent()) {
      return new ExecutionStatusWithDetails(ExecutionStatus.ERROR, statusDetail, noDaVinciStatusReported);
    }
    return new ExecutionStatusWithDetails(ExecutionStatus.STARTED, statusDetail, noDaVinciStatusReported);
  }
}
