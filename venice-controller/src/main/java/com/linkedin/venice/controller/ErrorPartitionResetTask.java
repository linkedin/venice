package com.linkedin.venice.controller;

import com.linkedin.venice.controller.stats.ErrorPartitionStats;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.helix.CachedReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixExternalViewRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;


/**
 * A task that iterates over store version resources and reset error partitions if they meet the following criteria:
 * 1. The store version resource is the current version.
 * 2. The store version is operating in ONLINE/OFFLINE Helix state model.
 * 3. The error partition only has exactly one error replica.
 */
public class ErrorPartitionResetTask implements Runnable, Closeable {
  private static final String TASK_ID_FORMAT = ErrorPartitionResetTask.class.getSimpleName() + " [cluster: %s] ";
  /**
   * Tracks auto reset attempts of applicable resources' error partitions. Automatically reset will only apply to
   * partitions of the current version resource with exactly one ERROR replica. The key is resource name and the value
   * is a map of partition number to reset attempt count.
   */
  private final Map<String, Map<Integer, Integer>> errorPartitionResetTracker = new HashMap<>();
  private final String taskId;
  private final Logger logger;
  private final String clusterName;
  private final HelixAdminClient helixAdminClient;
  private final CachedReadOnlyStoreRepository readOnlyStoreRepository;
  private final HelixExternalViewRepository routingDataRepository;
  private final int errorPartitionAutoResetLimit;
  private final long processingCycleDelayMs;
  private final ErrorPartitionStats errorPartitionStats;
  private final AtomicBoolean isRunning = new AtomicBoolean();
  private final Set<String> irrelevantResources = new HashSet<>();

  public ErrorPartitionResetTask(String clusterName, HelixAdminClient helixAdminClient,
      CachedReadOnlyStoreRepository readOnlyStoreRepository, HelixExternalViewRepository routingDataRepository,
      MetricsRepository metricsRepository, int errorPartitionAutoResetLimit, long processingCycleDelayMs) {
    taskId = String.format(TASK_ID_FORMAT, clusterName);
    logger = Logger.getLogger(taskId);
    this.clusterName = clusterName;
    this.helixAdminClient = helixAdminClient;
    this.readOnlyStoreRepository = readOnlyStoreRepository;
    this.routingDataRepository = routingDataRepository;
    this.errorPartitionAutoResetLimit = errorPartitionAutoResetLimit;
    this.processingCycleDelayMs = processingCycleDelayMs;
    errorPartitionStats = new ErrorPartitionStats(metricsRepository, clusterName);
  }

  @Override
  public void run() {
    logger.info("Running " + taskId);
    isRunning.set(true);
    while (isRunning.get()) {
      try {
        Utils.sleep(processingCycleDelayMs);
        long startTime = System.currentTimeMillis();
        // Copy the previous iteration's reset tracker resources and remove them from the set if they are still relevant
        irrelevantResources.addAll(errorPartitionResetTracker.keySet());
        readOnlyStoreRepository.getAllStores().stream().filter(s -> !s.isSystemStore())
            .forEach(this::resetApplicableErrorPartitions);

        // Remove all the irrelevant entries from the error partition reset tracker
        for (String irrelevantResource : irrelevantResources) {
          errorPartitionResetTracker.remove(irrelevantResource);
        }
        irrelevantResources.clear();
        errorPartitionStats.recordErrorPartitionProcessingTime(System.currentTimeMillis() - startTime);
      } catch (Exception e) {
        logger.error("Unexpected exception while running " + taskId, e);
        errorPartitionStats.recordErrorPartitionResetAttemptErrored();
      }
    }
    logger.info("Stopped " + taskId);
  }

  private void resetApplicableErrorPartitions(Store store) {
    int currentVersion = store.getCurrentVersion();
    if (currentVersion == Store.NON_EXISTING_VERSION || !store.getVersion(currentVersion).isPresent()
        || store.getVersion(currentVersion).get().isLeaderFollowerModelEnabled()) {
      // Currently, Leader/Follower state transitions are short transitions that's difficult to manifest ERROR
      // partitions due to transient Venice or Zk exceptions. This behavior may change if we ever introduce long
      // state transitions to Leader/Follower model.
      return;
    }
    String resourceName = store.getVersion(currentVersion).get().kafkaTopicName();
    irrelevantResources.remove(resourceName);
    try {
      PartitionAssignment partitionAssignment = routingDataRepository.getPartitionAssignments(resourceName);
      Map<Integer, Integer> partitionResetCountMap = errorPartitionResetTracker.computeIfAbsent(resourceName,
          k -> new HashMap<>());
      Map<String, List<String>> resetMap = new HashMap<>();
      for (Partition partition : partitionAssignment.getAllPartitions()) {
        List<Instance> errorInstances = partition.getErrorInstances();
        if (errorInstances.isEmpty()) {
          // Check if the partition reached healthy state after a reset.
          Set<String> states = partition.getAllInstances().keySet();
          if (states.size() == 1 && states.contains(HelixState.ONLINE_STATE)
              && partitionResetCountMap.containsKey(partition.getId())) {
            partitionResetCountMap.remove(partition.getId());
            errorPartitionStats.recordErrorPartitionRecoveredFromReset();
          }
        } else {
          // We are only interested in resetting error partitions with exactly 1 error replica. This is because we have
          // MIN_ACTIVE set to 2 and replication factor set to 3. If we have more than 2 error replicas then Helix
          // recovery rebalance should take care of it and it's best not to mess with it using a reset.

          Integer currentResetCount = partitionResetCountMap.getOrDefault(partition.getId(), 0);
          if (currentResetCount > errorPartitionAutoResetLimit) {
            // We have attempted and gave up on this partition and declared that it's unrecoverable from reset, skip.
            continue;
          }
          if (currentResetCount == errorPartitionAutoResetLimit) {
            // This partition is unrecoverable from reset either due to reset limit or too many error replicas.
            partitionResetCountMap.put(partition.getId(), errorPartitionAutoResetLimit + 1);
            errorPartitionStats.recordErrorPartitionUnrecoverableFromReset();
            logger.warn("Error partition unrecoverable from reset. Resource: " + resourceName + ", partition: "
                + partition.getId() + ", reset count: " + currentResetCount);
          } else if (errorInstances.size() > 1) {
            // The following scenarios can occur:
            // 1. Helix will trigger recovery re-balance in attempt to bring more replicas ONLINE.
            // 2. The recovery re-balance was successful and we now have excess error replicas that should be reset.
            //    e.g. 2 ERROR 3 ONLINE or 3 ERROR and 2 ONLINE for replication factor of 3 .
            // 3. All replicas are in ERROR state and Helix has given up on this partition until manual intervention.
            if (partition.getReadyToServeInstances().size() >= store.getReplicationFactor() - 1) {
              // Only perform reset for scenario 2 since the error partition reset task is not responsible for 1 and 3.
              partitionResetCountMap.put(partition.getId(), currentResetCount + 1);
              for (Instance i : errorInstances) {
                resetMap.computeIfAbsent(i.getNodeId(), k -> new ArrayList<>())
                    .add(HelixUtils.getPartitionName(resourceName, partition.getId()));
              }
            }
          } else {
            // Perform more resets.
            partitionResetCountMap.put(partition.getId(), currentResetCount + 1);
            resetMap.computeIfAbsent(errorInstances.get(0).getNodeId(), k -> new ArrayList<>())
                .add(HelixUtils.getPartitionName(resourceName, partition.getId()));
          }
        }
      }
      if (!partitionResetCountMap.isEmpty()) {
        errorPartitionResetTracker.put(resourceName, partitionResetCountMap);
      }
      resetMap.forEach((k,v) -> {
        helixAdminClient.resetPartition(clusterName, k, resourceName, v);
        errorPartitionStats.recordErrorPartitionResetAttempt(v.size());
      });
    } catch (VeniceNoHelixResourceException noHelixResourceException) {
      logger.error("Resource: " + resourceName + " is missing unexpectedly", noHelixResourceException);
      errorPartitionStats.recordErrorPartitionResetAttemptErrored();
    } catch (Exception e) {
      logger.error("Unexpected exception while processing partitions for resource: " + resourceName
          + " for error partition reset", e);
      errorPartitionStats.recordErrorPartitionResetAttemptErrored();
    }
  }

  @Override
  public void close() {
    isRunning.set(false);
  }
}
