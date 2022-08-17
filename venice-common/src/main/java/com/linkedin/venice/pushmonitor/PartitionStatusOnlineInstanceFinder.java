package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Find out online instances based on partition status. This is intended to be used to help router
 * find out available instances for L/F model resources.
 *
 * Since it listens to all partition status changes, ZK loads will increase dramatically.
 * We can use {@link HelixCustomizedViewOfflinePushRepository} instead to improve.
 */
public class PartitionStatusOnlineInstanceFinder
    implements PartitionStatusListener, OnlineInstanceFinder, VeniceResource, IZkChildListener {
  private static final Logger logger = LogManager.getLogger(PartitionStatusOnlineInstanceFinder.class);
  private final OfflinePushAccessor offlinePushAccessor;
  private final RoutingDataRepository routingDataRepository;
  private final ReadOnlyStoreRepository metadataRepo;

  private final Map<String, OfflinePushStatus> topicToResourceStatus;

  private final int retryStoreRefreshIntervalInMs = 5000;
  private final int retryStoreRefreshAttempt = 10;

  public PartitionStatusOnlineInstanceFinder(
      ReadOnlyStoreRepository metadataRepo,
      OfflinePushAccessor offlinePushAccessor,
      RoutingDataRepository routingDataRepository) {
    this.metadataRepo = metadataRepo;
    this.offlinePushAccessor = offlinePushAccessor;
    this.routingDataRepository = routingDataRepository;
    this.topicToResourceStatus = new VeniceConcurrentHashMap<>();
    refresh();
  }

  @Override
  public synchronized void onPartitionStatusChange(String kafkaTopic, ReadOnlyPartitionStatus partitionStatus) {
    partitionStatus.getReplicaStatuses()
        .forEach(
            replicaStatus -> logger
                .info("instance: {}, status: {}.", replicaStatus.getInstanceId(), replicaStatus.getCurrentStatus()));

    OfflinePushStatus offlinePushStatus = topicToResourceStatus.get(kafkaTopic);
    if (offlinePushStatus == null) {
      // have not yet received partition status for this topic yet. return;
      logger.info(
          "Instance finder received unknown partition status notification." + " Topic: " + kafkaTopic
              + ", Partition id: " + partitionStatus.getPartitionId() + ". Will ignore.");
      return;
    }

    if (!routingDataRepository.containsKafkaTopic(kafkaTopic)) {
      logger.warn(
          "Instance finder received partition status notification for topic unknown to RoutingDataRepository."
              + " Topic: " + kafkaTopic + ", Partition id: " + partitionStatus.getPartitionId());
      // ResourceAssignment and this class maintain their own state, and so things can get out of order. This forces the
      // resourceAssignment path to reconcile
      routingDataRepository.refreshRoutingDataForResource(kafkaTopic);
    }
    offlinePushStatus.setPartitionStatus(partitionStatus, false);
    try {
      if (!offlinePushStatus.getCurrentStatus().isTerminal()) {
        Pair<ExecutionStatus, Optional<String>> status = PushStatusDecider.getDecider(offlinePushStatus.getStrategy())
            .checkPushStatusAndDetailsByPartitionsStatus(
                offlinePushStatus,
                routingDataRepository.getPartitionAssignments(kafkaTopic));
        offlinePushStatus.updateStatus(status.getFirst());
      }
    } catch (VeniceNoHelixResourceException e) {
      /*
        This exception is thrown out if we cannot find corresponding EV
        when a OfflinePush update comes. This is expected since EV is
        slow in the heavy resource cluster and is likely that EV comes
        out later than OfflinePushStatus when new resources are created.
       */

      logger.warn("Can not find Helix resource: " + kafkaTopic + ", partition: " + partitionStatus.getPartitionId());
    }
  }

  /**
   * TODO: check if we need to cache the result since this method is called very frequently.
   * This method is in the critical read path; do not apply any global lock on it; use ConcurrentHashMap which has
   * a much fine-grained lock. It's okay to read stale data in a small time window.
   */
  @Override
  public List<Instance> getReadyToServeInstances(String kafkaTopic, int partitionId) {
    return getReadyToServeInstances(routingDataRepository.getPartitionAssignments(kafkaTopic), partitionId);
  }

  @Override
  public List<Instance> getReadyToServeInstances(PartitionAssignment partitionAssignment, int partitionId) {
    String kafkaTopic = partitionAssignment.getTopic();
    // Get the listing, but if we don't have it, try and refresh the information from zk
    OfflinePushStatus offlinePushStatus =
        topicToResourceStatus.computeIfAbsent(kafkaTopic, k -> getPushStatusFromZk(kafkaTopic));
    if (offlinePushStatus == null || partitionId >= partitionAssignment.getExpectedNumberOfPartitions()) {
      // have not received partition info related to this topic. Return empty list
      logger.warn(
          "Unknown partition id, partitionId=" + partitionId + ", partitionStatusCount="
              + (offlinePushStatus == null ? 0 : partitionAssignment.getExpectedNumberOfPartitions())
              + ", partitionCount=" + routingDataRepository.getNumberOfPartitions(kafkaTopic));
      return Collections.emptyList();
    }

    PartitionStatus partitionStatus = offlinePushStatus.getPartitionStatus(partitionId);

    if (partitionStatus == null) {
      logger.warn(
          String.format(
              "offline push status is incomplete. topic: %s, "
                  + "partition: %d is null. Try recovering it by reading from the ZK.",
              kafkaTopic,
              partitionId));
      OfflinePushStatus refreshedStatus = getPushStatusFromZk(kafkaTopic);
      topicToResourceStatus.put(kafkaTopic, refreshedStatus);
      partitionStatus = refreshedStatus.getPartitionStatus(partitionId);
    }

    return PushStatusDecider.getReadyToServeInstances(partitionStatus, partitionAssignment, partitionId);
  }

  @Override
  public Map<String, List<Instance>> getAllInstances(String kafkaTopic, int partitionId) {
    return routingDataRepository.getAllInstances(kafkaTopic, partitionId);
  }

  @Override
  public List<ReplicaState> getReplicaStates(String kafkaTopic, int partitionId) {
    OfflinePushStatus offlinePushStatus = topicToResourceStatus.get(kafkaTopic);
    if (offlinePushStatus == null || partitionId >= routingDataRepository.getNumberOfPartitions(kafkaTopic)) {
      logger.warn("Unable to find resource: " + kafkaTopic + " in the partition status list");
      throw new VeniceNoHelixResourceException(kafkaTopic);
    }

    if (partitionId != offlinePushStatus.getPartitionStatus(partitionId).getPartitionId()) {
      logger.warn(
          "Corrupted partition status list causing " + PartitionStatusOnlineInstanceFinder.class.getSimpleName()
              + " to retrieve the wrong PartitionStatus for partition: " + partitionId + " for resource: "
              + kafkaTopic);
      throw new VeniceNoHelixResourceException(kafkaTopic);
    }

    return getAllInstances(kafkaTopic, partitionId).entrySet()
        .stream()
        .flatMap(e -> e.getValue().stream().map(instance -> {
          ExecutionStatus executionStatus = PushStatusDecider.getReplicaCurrentStatus(
              offlinePushStatus.getPartitionStatus(partitionId).getReplicaHistoricStatusList(instance.getNodeId()));
          return new ReplicaState(
              partitionId,
              instance.getNodeId(),
              e.getKey(),
              executionStatus.toString(),
              executionStatus.equals(ExecutionStatus.COMPLETED));
        }))
        .collect(Collectors.toList());
  }

  @Override
  public int getNumberOfPartitions(String kafkaTopic) {
    return routingDataRepository.getNumberOfPartitions(kafkaTopic);
  }

  @Override
  public synchronized void refresh() {
    /**
     * Please be aware that the returned push status list is not ordered by partition Id; it's alphabetical order:
     * 0, 1, 10, 11, 12....
     */
    List<OfflinePushStatus> offlinePushStatusList = offlinePushAccessor.loadOfflinePushStatusesAndPartitionStatuses();
    clear();
    offlinePushStatusList.forEach(pushStatus -> {
      /*copy to a new list since the former is unmodifiable*/
      String topic = pushStatus.getKafkaTopic();
      topicToResourceStatus.put(topic, pushStatus);
      if (isLFModelEnabledForStoreVersion(topic, false)) {
        offlinePushAccessor.subscribePartitionStatusChange(pushStatus, this);
      }
    });

    offlinePushAccessor.subscribePushStatusCreationChange(this);
  }

  private boolean isLFModelEnabledForStoreVersion(String kafkaTopic, boolean isNewOfflinePushStatus) {
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    Store store = metadataRepo.getStore(storeName);
    if (store == null) {
      return false;
    }

    int storeVersion = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    Optional<Version> version = store.getVersion(storeVersion);

    if (!version.isPresent() && isNewOfflinePushStatus) {
      // In theory, the version should exist since the corresponding offline push status ZNode is newly created.
      // But there's a race condition that in-memory metadata hasn't been refreshed yet, so here refresh the store.
      logger.warn(String.format("Version %d does not exist in store %s. Refresh the store.", storeVersion, storeName));
      String logMessage = "still does not exist in store";
      int attempt = 0;
      while (attempt < retryStoreRefreshAttempt) {
        store = metadataRepo.refreshOneStore(storeName);
        version = store.getVersion(storeVersion);
        if (version.isPresent()) {
          break;
        }
        if (storeVersion <= store.getLargestUsedVersionNumber()) {
          /**
           * This is to handle an edge case: a version becomes error version and gets deleted quickly
           * (e.g. TestBatchForRocksDB.testDuplicateKey). If the version is already deleted by the time
           * PartitionStatusOnlineInstanceFinder handles new push status, no matter how many times the
           * store is refreshed, version is still not present. As a result, router will waste time on
           * refreshing store and synchronized methods will be blocked.
           *
           * Solution: end store refresh earlier when the version does not exist but version number <=
           * the largest used version number, which indicates that the version is deleted.
           */
          logMessage = "was delete from store";
          break;
        }
        attempt++;
        Utils.sleep(retryStoreRefreshIntervalInMs);
      }
      if (!version.isPresent()) {
        logger.warn(String.format("Version %d %s %s. Skip subscription.", storeVersion, logMessage, storeName));
        return false;
      }
      return version.get().isLeaderFollowerModelEnabled();
    }

    return version.map(Version::isLeaderFollowerModelEnabled).orElse(false);
  }

  @Override
  public synchronized void clear() {
    offlinePushAccessor.unsubscribePushStatusCreationChange(this);
    topicToResourceStatus.clear();
  }

  @Override
  public synchronized void handleChildChange(String parentPath, List<String> pushStatusList) {
    List<String> newPushStatusList = new ArrayList<>();
    Set<String> deletedPushStatusList = new HashSet<>(topicToResourceStatus.keySet());

    pushStatusList.forEach(pushStatusName -> {
      if (!topicToResourceStatus.containsKey(pushStatusName)) {
        newPushStatusList.add(pushStatusName);
      } else {
        deletedPushStatusList.remove(pushStatusName);
      }
    });

    newPushStatusList.forEach(pushStatusName -> {
      OfflinePushStatus status = getPushStatusFromZk(pushStatusName);
      if (status != null) {
        topicToResourceStatus.put(pushStatusName, status);
        if (isLFModelEnabledForStoreVersion(pushStatusName, true)) {
          offlinePushAccessor.subscribePartitionStatusChange(status, this);
        }
      }
    });

    deletedPushStatusList.forEach(pushStatusName -> {
      int partitionCount = topicToResourceStatus.get(pushStatusName).getNumberOfPartition();
      topicToResourceStatus.remove(pushStatusName);
      if (isLFModelEnabledForStoreVersion(pushStatusName, false)) {
        offlinePushAccessor.unsubscribePartitionsStatusChange(pushStatusName, partitionCount, this);
      }
    });
  }

  public ExecutionStatus getPushJobStatus(String kafkaTopic) {
    OfflinePushStatus offlinePushStatus = topicToResourceStatus.get(kafkaTopic);
    return offlinePushStatus == null ? ExecutionStatus.UNKNOWN : offlinePushStatus.getCurrentStatus();
  }

  private OfflinePushStatus getPushStatusFromZk(String kafkaTopic) {
    try {
      return offlinePushAccessor.getOfflinePushStatusAndItsPartitionStatuses(kafkaTopic);
    } catch (VeniceException exception) {
      logger.warn("Instance finder could not retrieve offline push status from ZK. Topic: " + kafkaTopic);
      return null;
    }
  }
}
