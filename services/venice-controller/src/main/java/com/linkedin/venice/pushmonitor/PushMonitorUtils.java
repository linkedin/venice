package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushstatushelper.PushStatusStoreReader;
import com.linkedin.venice.utils.Pair;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PushMonitorUtils {
  private static final Logger LOGGER = LogManager.getLogger(PushMonitorUtils.class);

  public static Pair<ExecutionStatus, String> getDaVinciPushStatusAndDetails(
      PushStatusStoreReader reader,
      String topicName,
      int partitionCount,
      Optional<String> incrementalPushVersion) {
    if (reader == null) {
      throw new VeniceException("D2Client must be provided to read from push status store.");
    }
    LOGGER.info("Getting Da Vinci push status for topic: {}", topicName);
    boolean allMiddleStatusReceived = true;
    ExecutionStatus completeStatus = incrementalPushVersion.isPresent()
        ? ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED
        : ExecutionStatus.COMPLETED;
    ExecutionStatus middleStatus = incrementalPushVersion.isPresent()
        ? ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED
        : ExecutionStatus.END_OF_PUSH_RECEIVED;
    Optional<String> erroredInstance = Optional.empty();
    String storeName = Version.parseStoreFromKafkaTopicName(topicName);
    int completedPartitions = 0;
    int totalInstanceCount = 0;
    int liveInstanceCount = 0;
    Set<Integer> incompletePartition = new HashSet<>();
    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
      Map<CharSequence, Integer> instances = reader.getPartitionStatus(
          storeName,
          Version.parseVersionFromVersionTopicName(topicName),
          partitionId,
          incrementalPushVersion);
      boolean allInstancesCompleted = true;
      totalInstanceCount += instances.size();
      for (Map.Entry<CharSequence, Integer> entry: instances.entrySet()) {
        ExecutionStatus status = ExecutionStatus.fromInt(entry.getValue());
        boolean isInstanceAlive = reader.isInstanceAlive(storeName, entry.getKey().toString());

        if (isInstanceAlive) {
          liveInstanceCount++;
        }
        if (status == middleStatus) {
          if (allInstancesCompleted && isInstanceAlive) {
            allInstancesCompleted = false;
          }
        } else if (status != completeStatus) {
          if (allInstancesCompleted || allMiddleStatusReceived) {
            if (isInstanceAlive) {
              allInstancesCompleted = false;
              allMiddleStatusReceived = false;
              if (status == ExecutionStatus.ERROR) {
                erroredInstance = Optional.of(entry.getKey().toString());
                break;
              }
            }
          }
        }
      }
      if (allInstancesCompleted) {
        completedPartitions++;
      } else {
        incompletePartition.add(partitionId);
      }
    }
    String statusDetail = null;
    String details = "";
    if (completedPartitions > 0) {
      details += completedPartitions + "/" + partitionCount + " partitions completed in" + totalInstanceCount
          + " Da Vinci instances.";
    }
    if (erroredInstance.isPresent()) {
      details += "Found a failed instance in Da Vinci: " + erroredInstance + ". live instances: " + liveInstanceCount
          + " total instances : " + totalInstanceCount;
    }
    int incompleteSize = incompletePartition.size();
    if (incompleteSize > 0 && incompleteSize <= 5) {
      details += ". Following partitions still not complete " + incompletePartition + ". live instances: "
          + liveInstanceCount + " total instances : " + totalInstanceCount;
    }
    if (details.length() != 0) {
      statusDetail = details;
    }
    if (completedPartitions == partitionCount) {
      return new Pair<>(completeStatus, statusDetail);
    } else if (allMiddleStatusReceived) {
      return new Pair<>(middleStatus, statusDetail);
    } else if (erroredInstance.isPresent()) {
      return new Pair<>(ExecutionStatus.ERROR, statusDetail);
    } else {
      return new Pair<>(ExecutionStatus.STARTED, statusDetail);
    }
  }
}
