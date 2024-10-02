package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This class contains some common util methods for push monitoring purpose.
 */
public class PushMonitorUtils {
  private static long daVinciErrorInstanceWaitTime = 5;

  private static final Map<String, Long> storeVersionToDVCDeadInstanceTimeMap = new ConcurrentHashMap<>();
  private static final Logger LOGGER = LogManager.getLogger(PushMonitorUtils.class);

  private static String getDVCIngestionErrorReason(ExecutionStatus errorReplicaStatus) {
    switch (errorReplicaStatus) {
      case DVC_INGESTION_ERROR_DISK_FULL:
        return " due to disk threshold reached";
      case DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED:
        return " due to memory limit reached";
      default:
        return "";
    }
  }

  /**
   * This method checks Da Vinci client push status for the target version from push status store and compute a final
   * status.
   *
   * It will try to get an aggregated status from version level status key first; if there is value, it will still try
   * to query the partition level keys, and combine the result with the old keys, in case DaVinci instances are
   * partially upgraded and some of them are still populating statuses with old keys.
   * If no value is found for the new key, it will fall back to partition level status key.
   *
   * A Da Vinci instance sent heartbeat to controllers recently is considered active. Only consider the push status
   * from active Da Vinci instances unless there are too many offline instances which triggers the fail fast mechanism.
   */
  public static ExecutionStatusWithDetails getDaVinciPushStatusAndDetails(
      PushStatusStoreReader reader,
      String topicName,
      int partitionCount,
      Optional<String> incrementalPushVersion,
      int maxOfflineInstanceCount,
      double maxOfflineInstanceRatio,
      boolean useDaVinciSpecificExecutionStatusForError) {
    if (reader == null) {
      throw new VeniceException("PushStatusStoreReader is null");
    }
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    int version = Version.parseVersionFromVersionTopicName(topicName);
    Map<CharSequence, Integer> instances = null;
    if (!incrementalPushVersion.isPresent()) {
      // For batch pushes, try to read from version level status key first.
      instances = reader.getVersionStatus(storeName, version);
    }
    if (instances == null) {
      // Fallback to partition level status key if version level status key is not found.
      return getDaVinciPartitionLevelPushStatusAndDetails(
          reader,
          topicName,
          partitionCount,
          incrementalPushVersion,
          maxOfflineInstanceCount,
          maxOfflineInstanceRatio,
          useDaVinciSpecificExecutionStatusForError);
    } else {
      // DaVinci starts using new status key format, which contains status for all partitions in one key.
      // Only batch pushes will use this key; incremental pushes will still use partition level status key.
      LOGGER.info("Getting Da Vinci version level push status for topic: {}", topicName);
      final int totalInstanceCount = instances.size();
      ExecutionStatus completeStatus = ExecutionStatus.COMPLETED;
      int completedInstanceCount = 0;
      boolean allInstancesCompleted = true;
      int liveInstanceCount = 0;
      int offlineInstanceCount = 0;
      Optional<String> erroredInstance = Optional.empty();
      Set<String> offlineInstanceList = new HashSet<>();
      Set<String> incompleteInstanceList = new HashSet<>();
      ExecutionStatus errorStatus = ExecutionStatus.ERROR;
      for (Map.Entry<CharSequence, Integer> entry: instances.entrySet()) {
        PushStatusStoreReader.InstanceStatus instanceStatus =
            reader.getInstanceStatus(storeName, entry.getKey().toString());
        if (instanceStatus.equals(PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING)) {
          LOGGER.info(
              "Skipping ingestion status report from bootstrapping instance: {} for topic: {}",
              entry.getKey().toString(),
              topicName);
          continue;
        }
        ExecutionStatus status = ExecutionStatus.valueOf(entry.getValue());
        // We will skip completed instances, as they have stopped emitting heartbeats and will not be counted as live
        // instances.
        if (status == completeStatus) {
          completedInstanceCount++;
          continue;
        }
        if (instanceStatus.equals(PushStatusStoreReader.InstanceStatus.DEAD)) {
          offlineInstanceCount++;
          // Keep at most 5 offline instances for logging purpose.
          if (offlineInstanceList.size() < 5) {
            offlineInstanceList.add(entry.getKey().toString());
          }
          continue;
        }
        // Derive the overall partition ingestion status based on all live replica ingestion status.
        liveInstanceCount++;
        allInstancesCompleted = false;
        if (status.isError()) {
          errorStatus = status;
          erroredInstance = Optional.of(entry.getKey().toString());
          break;
        }
        if (incompleteInstanceList.size() < 2) {
          // Keep at most 2 incomplete instances for logging purpose.
          incompleteInstanceList.add(entry.getKey().toString());
        }
      }

      boolean noDaVinciStatusReported = totalInstanceCount == 0;
      // Report error if too many Da Vinci instances are not alive for over 5 minutes.
      int maxOfflineInstanceAllowed =
          Math.max(maxOfflineInstanceCount, (int) (maxOfflineInstanceRatio * totalInstanceCount));
      if (offlineInstanceCount > maxOfflineInstanceAllowed) {
        Long lastUpdateTime = storeVersionToDVCDeadInstanceTimeMap.get(topicName);
        if (lastUpdateTime != null) {
          if (lastUpdateTime + TimeUnit.MINUTES.toMillis(daVinciErrorInstanceWaitTime) < System.currentTimeMillis()) {
            storeVersionToDVCDeadInstanceTimeMap.remove(topicName);
            return new ExecutionStatusWithDetails(
                useDaVinciSpecificExecutionStatusForError
                    ? ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES
                    : ExecutionStatus.ERROR,
                "Too many dead instances: " + offlineInstanceCount + ", total instances: " + totalInstanceCount
                    + ", example offline instances: " + offlineInstanceList,
                noDaVinciStatusReported);
          }
        } else {
          storeVersionToDVCDeadInstanceTimeMap.put(topicName, System.currentTimeMillis());
        }
      } else {
        storeVersionToDVCDeadInstanceTimeMap.remove(topicName);
      }

      StringBuilder statusDetailStringBuilder = new StringBuilder();
      if (completedInstanceCount > 0) {
        statusDetailStringBuilder.append(completedInstanceCount)
            .append("/")
            .append(totalInstanceCount)
            .append(" Da Vinci instances completed.");
      }
      if (erroredInstance.isPresent()) {
        statusDetailStringBuilder.append("Found a failed instance in Da Vinci. ")
            .append("Instance: ")
            .append(erroredInstance.get())
            .append(". Live instance count: ")
            .append(liveInstanceCount);
      }
      if (incompleteInstanceList.size() > 0) {
        statusDetailStringBuilder.append(". Some example incomplete instances ").append(incompleteInstanceList);
      }
      String statusDetail = statusDetailStringBuilder.toString();
      if (allInstancesCompleted) {
        // In case Da Vinci instances are partially upgraded to the release that produces version level status key,
        // we should always try to query the partition level status key for the old instances.
        ExecutionStatusWithDetails partitionLevelStatus = getDaVinciPartitionLevelPushStatusAndDetails(
            reader,
            topicName,
            partitionCount,
            incrementalPushVersion,
            maxOfflineInstanceCount,
            maxOfflineInstanceRatio,
            useDaVinciSpecificExecutionStatusForError);
        if (partitionLevelStatus.getStatus() != ExecutionStatus.COMPLETED) {
          // Do not report COMPLETED, instead, report status from the partition level status key.
          statusDetailStringBuilder.append(
              ". However, some instances are still reporting partition level status keys and they are not completed yet. ")
              .append(partitionLevelStatus.getDetails());
          return new ExecutionStatusWithDetails(
              partitionLevelStatus.getStatus(),
              statusDetailStringBuilder.toString(),
              false);
        }
        // TODO: Remove the above block after all Da Vinci instances are upgraded.
        storeVersionToDVCDeadInstanceTimeMap.remove(topicName);
        return new ExecutionStatusWithDetails(completeStatus, statusDetail, noDaVinciStatusReported);
      }
      if (erroredInstance.isPresent()) {
        storeVersionToDVCDeadInstanceTimeMap.remove(topicName);
        return new ExecutionStatusWithDetails(errorStatus, statusDetail, noDaVinciStatusReported);
      }
      return new ExecutionStatusWithDetails(ExecutionStatus.STARTED, statusDetail, noDaVinciStatusReported);
    }
  }

  /**
   * @Deprecated.
   * This method checks Da Vinci client push status of all partitions from push status store and compute a final status.
   * Inside each partition, this method will compute status based on all active Da Vinci instances.
   * A Da Vinci instance sent heartbeat to controllers recently is considered active.
   */
  public static ExecutionStatusWithDetails getDaVinciPartitionLevelPushStatusAndDetails(
      PushStatusStoreReader reader,
      String topicName,
      int partitionCount,
      Optional<String> incrementalPushVersion,
      int maxOfflineInstanceCount,
      double maxOfflineInstanceRatio,
      boolean useDaVinciSpecificExecutionStatusForError) {
    if (reader == null) {
      throw new VeniceException("PushStatusStoreReader is null");
    }
    LOGGER.info("Getting Da Vinci partition level push status for topic: {}", topicName);
    boolean allMiddleStatusReceived = true;
    ExecutionStatus completeStatus = incrementalPushVersion.isPresent()
        ? ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED
        : ExecutionStatus.COMPLETED;
    ExecutionStatus middleStatus = incrementalPushVersion.isPresent()
        ? ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED
        : ExecutionStatus.END_OF_PUSH_RECEIVED;
    Optional<String> erroredReplica = Optional.empty();
    int erroredPartitionId = 0;
    // initialize errorReplicaStatus to ERROR and then set specific error status if found
    ExecutionStatus errorReplicaStatus = ExecutionStatus.ERROR;
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    int version = Version.parseVersionFromVersionTopicName(topicName);
    int completedPartitions = 0;
    int totalReplicaCount = 0;
    int liveReplicaCount = 0;
    int completedReplicaCount = 0;
    Set<String> offlineInstanceList = new HashSet<>();
    Set<Integer> incompletePartition = new HashSet<>();
    /**
     * This cache is used to reduce the duplicate calls for liveness check as one host can host multiple partitions.
     */
    Map<String, PushStatusStoreReader.InstanceStatus> instanceLivenessCache = new HashMap<>();
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      Map<CharSequence, Integer> instances =
          reader.getPartitionStatus(storeName, version, partitionId, incrementalPushVersion);
      boolean allInstancesCompleted = true;
      totalReplicaCount += instances.size();
      for (Map.Entry<CharSequence, Integer> entry: instances.entrySet()) {
        String instanceName = entry.getKey().toString();
        PushStatusStoreReader.InstanceStatus instanceStatus = instanceLivenessCache
            .computeIfAbsent(instanceName, ignored -> reader.getInstanceStatus(storeName, instanceName));
        if (instanceStatus.equals(PushStatusStoreReader.InstanceStatus.BOOTSTRAPPING)) {
          // Don't count bootstrapping instance status report.
          totalReplicaCount--;
          LOGGER.info(
              "Skipping ingestion status report from bootstrapping node: {} for topic: {}, partition: {}",
              entry.getKey().toString(),
              topicName,
              partitionId);
          continue;
        }

        ExecutionStatus status = ExecutionStatus.valueOf(entry.getValue());
        // We will skip completed replicas, as they have stopped emitting heartbeats and will not be counted as live
        // replicas.
        if (status == completeStatus) {
          completedReplicaCount++;
          continue;
        }
        if (instanceStatus.equals(PushStatusStoreReader.InstanceStatus.DEAD)) {
          // Keep at most 5 offline instances for logging purpose.
          if (offlineInstanceList.size() < 5) {
            offlineInstanceList.add(entry.getKey().toString());
          }
          continue;
        }
        // Derive the overall partition ingestion status based on all live replica ingestion status.
        liveReplicaCount++;
        if (status == middleStatus) {
          allInstancesCompleted = false;
          continue;
        }
        allInstancesCompleted = false;
        allMiddleStatusReceived = false;
        if (status.isError()) {
          erroredReplica = Optional.of(entry.getKey().toString());
          erroredPartitionId = partitionId;
          errorReplicaStatus = status;
          break;
        }
      }
      if (allInstancesCompleted) {
        completedPartitions++;
      } else {
        incompletePartition.add(partitionId);
      }
    }
    boolean noDaVinciStatusReported = totalReplicaCount == 0;
    int offlineReplicaCount = totalReplicaCount - liveReplicaCount - completedReplicaCount;
    // Report error if too many Da Vinci instances are not alive for over 5 minutes.
    int maxOfflineInstanceAllowed =
        Math.max(maxOfflineInstanceCount, (int) (maxOfflineInstanceRatio * totalReplicaCount));
    if (offlineReplicaCount > maxOfflineInstanceAllowed) {
      Long lastUpdateTime = storeVersionToDVCDeadInstanceTimeMap.get(topicName);
      if (lastUpdateTime != null) {
        if (lastUpdateTime + TimeUnit.MINUTES.toMillis(daVinciErrorInstanceWaitTime) < System.currentTimeMillis()) {
          storeVersionToDVCDeadInstanceTimeMap.remove(topicName);
          return new ExecutionStatusWithDetails(
              useDaVinciSpecificExecutionStatusForError
                  ? ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES
                  : ExecutionStatus.ERROR,
              "Too many dead instances: " + offlineReplicaCount + ", total instances: " + totalReplicaCount
                  + ", example offline instances: " + offlineInstanceList,
              noDaVinciStatusReported);
        }
      } else {
        storeVersionToDVCDeadInstanceTimeMap.put(topicName, System.currentTimeMillis());
      }
    } else {
      storeVersionToDVCDeadInstanceTimeMap.remove(topicName);
    }

    StringBuilder statusDetailStringBuilder = new StringBuilder();
    if (completedPartitions > 0) {
      statusDetailStringBuilder.append(completedPartitions)
          .append("/")
          .append(partitionCount)
          .append(" partitions completed in ")
          .append(totalReplicaCount)
          .append(" Da Vinci replicas.");
    }
    if (erroredReplica.isPresent()) {
      statusDetailStringBuilder.append("Found a failed partition replica in Da Vinci")
          .append(getDVCIngestionErrorReason(errorReplicaStatus))
          .append(". ")
          .append("Partition: ")
          .append(erroredPartitionId)
          .append(" Replica: ")
          .append(erroredReplica.get())
          .append(". Live replica count: ")
          .append(liveReplicaCount)
          .append(", completed replica count: ")
          .append(completedReplicaCount)
          .append(", total replica count: ")
          .append(totalReplicaCount);
    }
    int incompleteSize = incompletePartition.size();
    if (incompleteSize > 0 && incompleteSize <= 5) {
      statusDetailStringBuilder.append(". Following partitions still not complete ")
          .append(incompletePartition)
          .append(". Live replica count: ")
          .append(liveReplicaCount)
          .append(", completed replica count: ")
          .append(completedReplicaCount)
          .append(", total replica count: ")
          .append(totalReplicaCount);
    }
    String statusDetail = statusDetailStringBuilder.toString();
    if (completedPartitions == partitionCount) {
      storeVersionToDVCDeadInstanceTimeMap.remove(topicName);
      return new ExecutionStatusWithDetails(completeStatus, statusDetail, noDaVinciStatusReported);
    }
    if (allMiddleStatusReceived) {
      return new ExecutionStatusWithDetails(middleStatus, statusDetail, noDaVinciStatusReported);
    }
    if (erroredReplica.isPresent()) {
      storeVersionToDVCDeadInstanceTimeMap.remove(topicName);
      return new ExecutionStatusWithDetails(errorReplicaStatus, statusDetail, noDaVinciStatusReported);
    }
    return new ExecutionStatusWithDetails(ExecutionStatus.STARTED, statusDetail, noDaVinciStatusReported);
  }

  static void setDaVinciErrorInstanceWaitTime(int time) {
    daVinciErrorInstanceWaitTime = time;
  }

  // For testing purpose
  static void setDVCDeadInstanceTime(String topicName, long timestamp) {
    storeVersionToDVCDeadInstanceTimeMap.put(topicName, timestamp);
  }
}
