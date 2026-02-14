package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.PushStatusCleanUpServiceState.FAILED;
import static com.linkedin.venice.pushmonitor.PushStatusCleanUpServiceState.RUNNING;
import static com.linkedin.venice.pushmonitor.PushStatusCleanUpServiceState.STOPPED;

import com.linkedin.venice.controller.HelixVeniceClusterResources;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.LatencyUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * LeakedPushStatusCleanUpService will wake up regularly (interval is determined by controller config
 * {@link com.linkedin.venice.ConfigKeys#LEAKED_PUSH_STATUS_CLEAN_UP_SERVICE_SLEEP_INTERVAL_MS}), get all existing push
 * status ZNodes on Zookeeper that belong to the specified cluster, without scanning through the replica statuses, find
 * all leaked push status and delete them on Zookeeper.
 *
 * The life cycle of LeakedPushStatusCleanUpService matches the life cycle of {@link HelixVeniceClusterResources},
 * meaning that there is one clean up service for each cluster, and it's built when the controller is promoted to leader
 * role for the cluster.
 *
 * The clean up service is needed because push job killing in server nodes is asynchronous, it's possible that servers
 * write push status after controllers think they have cleaned up the push status.
 */
public class LeakedPushStatusCleanUpService extends AbstractVeniceService {
  private static final Logger LOGGER = LogManager.getLogger(LeakedPushStatusCleanUpService.class);
  private static final Comparator<Integer> VERSION_COMPARATOR = new Comparator<Integer>() {
    @Override
    public int compare(Integer o1, Integer o2) {
      /**
       * Higher version number comes first.
       */
      return o2 - o1;
    }
  };

  /**
   * Keep 1 leaked push status version for debugging.
   */
  private static final int MAX_LEAKED_VERSION_TO_KEEP = 1;

  private final String clusterName;
  private final OfflinePushAccessor offlinePushAccessor;
  private final ReadOnlyStoreRepository metadataRepository;
  private final StoreCleaner storeCleaner;
  private final AggPushStatusCleanUpStats aggPushStatusCleanUpStats;
  private final long sleepIntervalInMs;
  private final long leakedResourceAllowedLingerTimeInMs;
  private final Thread cleanupThread;
  private final AtomicBoolean stop = new AtomicBoolean(false);
  private final RoutingDataRepository routingDataRepository;

  public LeakedPushStatusCleanUpService(
      String clusterName,
      OfflinePushAccessor offlinePushAccessor,
      ReadOnlyStoreRepository metadataRepository,
      StoreCleaner storeCleaner,
      AggPushStatusCleanUpStats aggPushStatusCleanUpStats,
      long sleepIntervalInMs,
      long leakedResourceAllowedLingerTimeInMs,
      RoutingDataRepository routingDataRepository) {
    this.clusterName = clusterName;
    this.offlinePushAccessor = offlinePushAccessor;
    this.metadataRepository = metadataRepository;
    this.storeCleaner = storeCleaner;
    this.aggPushStatusCleanUpStats = aggPushStatusCleanUpStats;
    this.sleepIntervalInMs = sleepIntervalInMs;
    this.leakedResourceAllowedLingerTimeInMs = leakedResourceAllowedLingerTimeInMs;
    this.routingDataRepository = routingDataRepository;
    this.cleanupThread = new Thread(new PushStatusCleanUpTask());
  }

  @Override
  public boolean startInner() throws Exception {
    cleanupThread.start();
    return true;
  }

  @Override
  public void stopInner() throws Exception {
    stop.set(true);
    cleanupThread.interrupt();
  }

  /**
   * Helper function; group store versions by store name.
   * @param storeVersions
   * @return a map; key: storeName, value: a list of the store's version numbers observed on push status ZK path
   */
  private static Map<String, PriorityQueue<Integer>> groupVersionsByStore(List<String> storeVersions) {
    Map<String, PriorityQueue<Integer>> storeToVersions = new HashMap<>();
    for (String storeVersion: storeVersions) {
      if (!Version.isVersionTopic(storeVersion)) {
        LOGGER.warn("Found an invalid push status path: {}", storeVersion);
        continue;
      }
      int version = Version.parseVersionFromKafkaTopicName(storeVersion);
      String storeName = Version.parseStoreFromKafkaTopicName(storeVersion);
      storeToVersions.computeIfAbsent(storeName, n -> new PriorityQueue<>(VERSION_COMPARATOR));
      storeToVersions.computeIfPresent(storeName, (key, queue) -> {
        if (!queue.contains(version)) {
          queue.offer(version);
        }
        return queue;
      });
    }
    return storeToVersions;
  }

  /**
   * Clean up stale replica statuses from partition statuses for the given topic.
   * Stale replicas are those that are no longer assigned to the partition according to current Helix assignment.
   *
   * @param kafkaTopic the Kafka topic to clean up stale replicas for
   */
  private void cleanupStaleReplicaStatuses(String kafkaTopic) {
    try {
      // Check if routing data exists for this topic
      if (!routingDataRepository.containsKafkaTopic(kafkaTopic)) {
        LOGGER.debug("No routing data found for topic {}, skipping stale replica cleanup", kafkaTopic);
        return;
      }

      // Get current partition assignment from Helix
      PartitionAssignment partitionAssignment = routingDataRepository.getPartitionAssignments(kafkaTopic);
      if (partitionAssignment == null) {
        LOGGER.debug("No partition assignment found for topic {}, skipping stale replica cleanup", kafkaTopic);
        return;
      }

      // Get the offline push status
      OfflinePushStatus offlinePushStatus = offlinePushAccessor.getOfflinePushStatusAndItsPartitionStatuses(kafkaTopic);
      if (offlinePushStatus == null) {
        LOGGER.debug("No offline push status found for topic {}, skipping stale replica cleanup", kafkaTopic);
        return;
      }

      int totalStaleReplicasRemoved = 0;

      // For each partition, clean up stale replicas
      for (PartitionStatus partitionStatus: offlinePushStatus.getPartitionStatuses()) {
        int partitionId = partitionStatus.getPartitionId();

        // Get current instances assigned to this partition
        Partition partition = partitionAssignment.getPartition(partitionId);
        if (partition == null) {
          LOGGER.warn("Partition {} not found in partition assignment for topic {}", partitionId, kafkaTopic);
          continue;
        }

        // Get set of currently assigned instance IDs
        Set<String> currentInstanceIds =
            partition.getAllInstancesSet().stream().map(Instance::getNodeId).collect(Collectors.toSet());

        // Get replica statuses and identify stale ones
        Collection<ReplicaStatus> replicaStatuses = partitionStatus.getReplicaStatuses();
        Set<String> existingInstanceIds =
            replicaStatuses.stream().map(ReplicaStatus::getInstanceId).collect(Collectors.toSet());

        // Find stale replicas (in push status but not in current assignment)
        Set<String> staleInstanceIds = new HashSet<>(existingInstanceIds);
        staleInstanceIds.removeAll(currentInstanceIds);

        if (!staleInstanceIds.isEmpty()) {
          LOGGER.info(
              "Found {} stale replica(s) in topic {} partition {}: {}. Current instances: {}",
              staleInstanceIds.size(),
              kafkaTopic,
              partitionId,
              staleInstanceIds,
              currentInstanceIds);

          // Create a new PartitionStatus with only current replicas
          PartitionStatus updatedPartitionStatus = new PartitionStatus(partitionId);
          for (ReplicaStatus replicaStatus: replicaStatuses) {
            if (currentInstanceIds.contains(replicaStatus.getInstanceId())) {
              // Keep this replica - it's still assigned
              updatedPartitionStatus.updateReplicaStatus(
                  replicaStatus.getInstanceId(),
                  replicaStatus.getCurrentStatus(),
                  replicaStatus.getIncrementalPushVersion());

              // Copy status history
              updatedPartitionStatus.getReplicaStatuses()
                  .stream()
                  .filter(rs -> rs.getInstanceId().equals(replicaStatus.getInstanceId()))
                  .findFirst()
                  .ifPresent(rs -> rs.setStatusHistory(new ArrayList<>(replicaStatus.getStatusHistory())));
            }
          }

          // Update the partition status in ZK with cleaned up replica statuses
          offlinePushAccessor.updatePartitionStatus(kafkaTopic, updatedPartitionStatus);

          totalStaleReplicasRemoved += staleInstanceIds.size();
          LOGGER.info(
              "Removed {} stale replica(s) from topic {} partition {}",
              staleInstanceIds.size(),
              kafkaTopic,
              partitionId);
        }
      }

      if (totalStaleReplicasRemoved > 0) {
        LOGGER.info(
            "Cleaned up {} total stale replica(s) from topic {} in cluster {}",
            totalStaleReplicasRemoved,
            kafkaTopic,
            clusterName);
      }
    } catch (Exception e) {
      LOGGER.error("Error cleaning up stale replica statuses for topic {}", kafkaTopic, e);
    }
  }

  private class PushStatusCleanUpTask implements Runnable {
    @Override
    public void run() {
      aggPushStatusCleanUpStats.recordLeakedPushStatusCleanUpServiceState(RUNNING);
      while (!stop.get()) {
        try {
          /**
           * Load all push status paths in the cluster.
           */
          List<String> pushStatusPaths = offlinePushAccessor.loadOfflinePushStatusPaths();
          Map<String, PriorityQueue<Integer>> storeToVersions = groupVersionsByStore(pushStatusPaths);
          for (Map.Entry<String, PriorityQueue<Integer>> entry: storeToVersions.entrySet()) {
            String storeName = entry.getKey();
            PriorityQueue<Integer> versions = entry.getValue();
            List<String> leakedPushStatuses = new ArrayList<>();
            try {
              final Store store = metadataRepository.getStoreOrThrow(storeName);
              int leakedPushStatusCounter = 0;
              while (!versions.isEmpty()) {
                int version = versions.poll();
                /**
                 * Push status version bigger than current version could be an ongoing push job, can't define whether it's
                 * a leaked push status without deserializing the push status ZNode; so only focus on cleaning up leaked
                 * push status before the current version.
                 *
                 * If a version is not in store config anymore, it indicates the push is not being monitored; keep at most
                 * {@link MAX_LEAKED_VERSION_TO_KEEP} leaked push status for debugging; the rest will be deleted.
                 */
                if (version < store.getCurrentVersion() && !store.containsVersion(version)) {
                  String kafkaTopic = Version.composeKafkaTopic(storeName, version);
                  if (leakedPushStatusCounter++ < MAX_LEAKED_VERSION_TO_KEEP) {
                    // If the leaked resources have been lingering for a while, we should still delete them.
                    offlinePushAccessor.getOfflinePushStatusCreationTime(kafkaTopic).ifPresent(creationTime -> {
                      long lingerTime = LatencyUtils.getElapsedTimeFromMsToMs(creationTime);
                      if (lingerTime > leakedResourceAllowedLingerTimeInMs) {
                        logger.info(
                            "The leaked push status has been lingering over {}ms, add to deletion list: {}",
                            leakedResourceAllowedLingerTimeInMs,
                            kafkaTopic);
                        leakedPushStatuses.add(Version.composeKafkaTopic(storeName, version));
                      } else {
                        logger.info(
                            "Keep the leaked push status for investigation, so not deleting: {}, linger time: {}ms",
                            kafkaTopic,
                            lingerTime);
                      }
                    });
                    continue;
                  }
                  logger.info("Found a leaked push status: {} in cluster {}", kafkaTopic, clusterName);
                  leakedPushStatuses.add(kafkaTopic);
                }
              }

              /**
               * Delete all leaked push statuses
               */
              leakedPushStatuses.stream().forEach(kafkaTopic -> {
                logger.info("Deleting leaked push status: {} in cluster {}", kafkaTopic, clusterName);
                if (storeCleaner.containsHelixResource(clusterName, kafkaTopic)) {
                  logger.info("Store version {} is also leaked in Helix, delete it through Helix", kafkaTopic);
                  storeCleaner.deleteHelixResource(clusterName, kafkaTopic);
                } else {
                  offlinePushAccessor.deleteOfflinePushStatusAndItsPartitionStatuses(kafkaTopic);
                }
              });
              aggPushStatusCleanUpStats.recordLeakedPushStatusCount(leakedPushStatusCounter);
              aggPushStatusCleanUpStats.recordSuccessfulLeakedPushStatusCleanUpCount(leakedPushStatuses.size());
            } catch (Throwable e) {
              /**
               * Don't stop the service for one single store
               */
              LOGGER.error(
                  "{} doesn't exist in metadata repo in cluster {} but has leaked push status: {}",
                  storeName,
                  clusterName,
                  Version.composeKafkaTopic(storeName, versions.iterator().next()),
                  e);
              aggPushStatusCleanUpStats.recordFailedLeakedPushStatusCleanUpCount(leakedPushStatuses.size());
            }
          }

          /**
           * Clean up stale replica statuses for all active push statuses
           */
          if (routingDataRepository != null) {
            for (String kafkaTopic: pushStatusPaths) {
              if (Version.isVersionTopic(kafkaTopic)) {
                cleanupStaleReplicaStatuses(kafkaTopic);
              }
            }
          }

          /**
           * Avoid busy scanning.
           */
          Thread.sleep(sleepIntervalInMs);
          if (stop.get()) {
            aggPushStatusCleanUpStats.recordLeakedPushStatusCleanUpServiceState(STOPPED);
            break;
          }
        } catch (InterruptedException e) {
          LOGGER.warn("Received InterruptedException during sleep in LeakedPushStatusCleanUpService thread.");
          aggPushStatusCleanUpStats.recordLeakedPushStatusCleanUpServiceState(STOPPED);
          break;
        } catch (Throwable e) {
          LOGGER.error("Error in push status clean-up task", e);
          aggPushStatusCleanUpStats.recordLeakedPushStatusCleanUpServiceState(FAILED);
          break;
        }
      }
      LOGGER.info("Push status clean-up task stopped");
    }
  }
}
