package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.I0Itec.zkclient.IZkChildListener;
import org.apache.log4j.Logger;


/**
 * Find out online instances based on partition status. This is intended to be used to help router
 * find out available instances for L/F model resources.
 *
 * TODO: Since it listens to all partition status changes, ZK loads will increase dramatically.
 * We should be cautious about it and ramp up resources gradually in case it impacts router's performance.
 */
public class PartitionStatusOnlineInstanceFinder
    implements OfflinePushAccessor.PartitionStatusListener, OnlineInstanceFinder, VeniceResource, IZkChildListener {

  private static final Logger logger = Logger.getLogger(PartitionStatusOnlineInstanceFinder.class);
  private final OfflinePushAccessor offlinePushAccessor;
  private final RoutingDataRepository routingDataRepository;
  private final ReadOnlyStoreRepository metadataRepo;

  private final Map<String, OfflinePushStatus> topicToResourceStatus;

  public PartitionStatusOnlineInstanceFinder(ReadOnlyStoreRepository metadataRepo,
      OfflinePushAccessor offlinePushAccessor, RoutingDataRepository routingDataRepository) {
    this.metadataRepo = metadataRepo;
    this.offlinePushAccessor = offlinePushAccessor;
    this.routingDataRepository = routingDataRepository;
    this.topicToResourceStatus = new VeniceConcurrentHashMap<>();
    refresh();
  }

  @Override
  public synchronized void onPartitionStatusChange(String kafkaTopic, ReadOnlyPartitionStatus partitionStatus) {
    OfflinePushStatus offlinePushStatus = topicToResourceStatus.get(kafkaTopic);
    if (offlinePushStatus == null ) {
      // have not yet received partition status for this topic yet. return;
      logger.info("Instance finder received unknown partition status notification." +
          " Topic: " + kafkaTopic + ", Partition id: " + partitionStatus.getPartitionId() + ". Will ignore.");
      return;
    }

    if (!routingDataRepository.containsKafkaTopic(kafkaTopic)) {
      logger.warn("Instance finder received partition status notification for topic unknown to RoutingDataRepository." +
          " Topic: " + kafkaTopic + ", Partition id: " + partitionStatus.getPartitionId());
    }
    offlinePushStatus.setPartitionStatus(partitionStatus, false);
    if (!offlinePushStatus.getCurrentStatus().isTerminal()) {
      Pair<ExecutionStatus, Optional<String>> status = PushStatusDecider.getDecider(offlinePushStatus.getStrategy())
          .checkPushStatusAndDetailsByPartitionsStatus(offlinePushStatus, routingDataRepository.getPartitionAssignments(kafkaTopic));
      offlinePushStatus.updateStatus(status.getFirst());
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
    OfflinePushStatus offlinePushStatus = topicToResourceStatus.get(kafkaTopic);
    if (offlinePushStatus == null || partitionId >= partitionAssignment.getExpectedNumberOfPartitions()) {
      // have not received partition info related to this topic. Return empty list
      logger.warn("Unknown partition id, partitionId=" + partitionId +
          ", partitionStatusCount=" + (offlinePushStatus == null ? 0 : partitionAssignment.getExpectedNumberOfPartitions()) +
          ", partitionCount=" + routingDataRepository.getNumberOfPartitions(kafkaTopic));
      return Collections.emptyList();
    }

    PartitionStatus partitionStatus = offlinePushStatus.getPartitionStatus(partitionId);
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
      logger.warn("Corrupted partition status list causing " + PartitionStatusOnlineInstanceFinder.class.getSimpleName()
          + " to retrieve the wrong PartitionStatus for partition: " + partitionId + " for resource: " + kafkaTopic);
      throw new VeniceNoHelixResourceException(kafkaTopic);
    }

    return getAllInstances(kafkaTopic, partitionId).entrySet().stream()
        .flatMap(e -> e.getValue().stream()
            .map(instance -> {
              ExecutionStatus executionStatus = PushStatusDecider.getReplicaCurrentStatus(
                  offlinePushStatus.getPartitionStatus(partitionId).getReplicaHistoricStatusList(instance.getNodeId()));
              return new ReplicaState(partitionId, instance.getNodeId(), e.getKey(), executionStatus.toString(),
                  executionStatus.equals(ExecutionStatus.COMPLETED));
            })).collect(Collectors.toList());
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
      if (isLFModelEnabledForStoreVersion(topic)) {
        offlinePushAccessor.subscribePartitionStatusChange(pushStatus, this);
      }
    });

    offlinePushAccessor.subscribePushStatusCreationChange(this);
  }

  private boolean isLFModelEnabledForStoreVersion(String kafkaTopic) {
    Store store = metadataRepo.getStore(Version.parseStoreFromKafkaTopicName(kafkaTopic));
    if (store == null) {
      return false;
    }

    Optional<Version> version = store.getVersion(Version.parseVersionFromKafkaTopicName(kafkaTopic));
    return version
        .map(Version::isLeaderFollowerModelEnabled)
        .orElse(false);
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
        if (isLFModelEnabledForStoreVersion(pushStatusName)) {
          offlinePushAccessor.subscribePartitionStatusChange(status, this);
        }
      }
    });

    deletedPushStatusList.forEach(pushStatusName -> {
      int partitionCount = topicToResourceStatus.get(pushStatusName).getNumberOfPartition();
      topicToResourceStatus.remove(pushStatusName);
      if (isLFModelEnabledForStoreVersion(pushStatusName)) {
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
