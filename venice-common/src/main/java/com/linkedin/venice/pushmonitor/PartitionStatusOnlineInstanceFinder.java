package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoHelixResourceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.routerapi.ReplicaState;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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

  // TODO: We should use a Map of PartitionStatus that is similar to idToPartitionMap in PartitionAssignment instead of
  //  a List so we can still get partial results even when some partitions are missing.
  private final Map<String, List<PartitionStatus>> topicToPartitionMap;

  public PartitionStatusOnlineInstanceFinder(ReadOnlyStoreRepository metadataRepo,
      OfflinePushAccessor offlinePushAccessor, RoutingDataRepository routingDataRepository) {
    this.metadataRepo = metadataRepo;
    this.offlinePushAccessor = offlinePushAccessor;
    this.routingDataRepository = routingDataRepository;
    this.topicToPartitionMap = new HashMap<>();
    refresh();
  }

  @Override
  public synchronized void onPartitionStatusChange(String kafkaTopic, ReadOnlyPartitionStatus partitionStatus) {
    List<PartitionStatus> statusList = topicToPartitionMap.get(kafkaTopic);
    if (statusList == null ) {
      // have not yet received partition status for this topic yet. return;
      logger.info("Instance finder received unknown partition status notification." +
          " Topic: " + kafkaTopic + ", Partition id: " + partitionStatus.getPartitionId() + ". Will ignore.");
      return;
    }

    if (routingDataRepository.containsKafkaTopic(kafkaTopic)) {
      if (partitionStatus.getPartitionId() >= routingDataRepository.getNumberOfPartitions(kafkaTopic)) {
        logger.error("Received an invalid partition:" + partitionStatus.getPartitionId() + " for topic:" + kafkaTopic);
      }
    } else {
      logger.warn("Instance finder received partition status notification for topic unknown to RoutingDataRepository." +
          " Topic: " + kafkaTopic + ", Partition id: " + partitionStatus.getPartitionId());
    }
    OfflinePushStatus.setPartitionStatus(statusList, partitionStatus, kafkaTopic);
  }

  /**
   * TODO: check if we need to cache the result since this method is called very frequently.
   * The method is synchronized with other methods that modify topicToPartitionMap.
   */
  @Override
  public synchronized List<Instance> getReadyToServeInstances(String kafkaTopic, int partitionId) {
    return getReadyToServeInstances(routingDataRepository.getPartitionAssignments(kafkaTopic), partitionId);
  }

  @Override
  public synchronized List<Instance> getReadyToServeInstances(PartitionAssignment partitionAssignment, int partitionId) {
    String kafkaTopic = partitionAssignment.getTopic();
    List<PartitionStatus> statusList = topicToPartitionMap.get(kafkaTopic);
    if (statusList == null || partitionId >= statusList.size()) {
      // have not received partition info related to this topic. Return empty list
      logger.warn("Unknown partition id, partitionId=" + partitionId +
          ", partitionStatusCount=" + (statusList == null ? 0 : statusList.size()) +
          ", partitionCount=" + routingDataRepository.getNumberOfPartitions(kafkaTopic));
      return Collections.emptyList();
    }

    PartitionStatus partitionStatus = statusList.get(partitionId);
    return PushStatusDecider.getReadyToServeInstances(partitionStatus, partitionAssignment, partitionId);
  }

  @Override
  public Map<String, List<Instance>> getAllInstances(String kafkaTopic, int partitionId) {
    return routingDataRepository.getAllInstances(kafkaTopic, partitionId);
  }

  @Override
  public List<ReplicaState> getReplicaStates(String kafkaTopic, int partitionId) {
    List<PartitionStatus> partitionStatusList = topicToPartitionMap.get(kafkaTopic);
    if (partitionStatusList == null || partitionId >= partitionStatusList.size()) {
      logger.warn("Unable to find resource: " + kafkaTopic + " in the partition status list");
      throw new VeniceNoHelixResourceException(kafkaTopic);
    }

    if (partitionId != partitionStatusList.get(partitionId).getPartitionId()) {
      logger.warn("Corrupted partition status list causing " + PartitionStatusOnlineInstanceFinder.class.getSimpleName()
          + " to retrieve the wrong PartitionStatus for partition: " + partitionId + " for resource: " + kafkaTopic);
      throw new VeniceNoHelixResourceException(kafkaTopic);
    }

    return getAllInstances(kafkaTopic, partitionId).entrySet().stream()
        .flatMap(e -> e.getValue().stream()
            .map(instance -> {
              ExecutionStatus executionStatus = PushStatusDecider.getReplicaCurrentStatus(
                  partitionStatusList.get(partitionId).getReplicaHistoricStatusList(instance.getNodeId()));
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
    List<OfflinePushStatus> offlinePushStatusList = offlinePushAccessor.loadOfflinePushStatusesAndPartitionStatuses();
    clear();
    offlinePushStatusList.forEach(pushStatus -> {
      /*copy to a new list since the former is unmodifiable*/
      String topic = pushStatus.getKafkaTopic();
      topicToPartitionMap.put(topic, new ArrayList<>(pushStatus.getPartitionStatuses()));
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
    topicToPartitionMap.clear();
  }

  @Override
  public synchronized void handleChildChange(String parentPath, List<String> pushStatusList) {
    List<String> newPushStatusList = new ArrayList<>();
    Set<String> deletedPushStatusList = new HashSet<>(topicToPartitionMap.keySet());

    pushStatusList.forEach(pushStatusName -> {
      if (!topicToPartitionMap.containsKey(pushStatusName)) {
        newPushStatusList.add(pushStatusName);
      } else {
        deletedPushStatusList.remove(pushStatusName);
      }
    });

    newPushStatusList.forEach(pushStatusName -> {
      OfflinePushStatus status = getPushStatusFromZk(pushStatusName);
      if (status != null) {
        topicToPartitionMap.put(pushStatusName, new ArrayList<>(status.getPartitionStatuses()));
        if (isLFModelEnabledForStoreVersion(pushStatusName)) {
          offlinePushAccessor.subscribePartitionStatusChange(status, this);
        }
      }
    });

    deletedPushStatusList.forEach(pushStatusName -> {
      List<PartitionStatus> statusList = topicToPartitionMap.get(pushStatusName);
      topicToPartitionMap.remove(pushStatusName);
      if (isLFModelEnabledForStoreVersion(pushStatusName)) {
        offlinePushAccessor.unsubscribePartitionsStatusChange(pushStatusName, statusList.size(), this);
      }
    });
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
