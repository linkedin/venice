package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.VeniceResource;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.RoutingDataRepository;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
    implements OfflinePushAccessor.PartitionStatusListener, OnlineInstanceFinder,
               VeniceResource, IZkChildListener {
  private static final Logger logger = Logger.getLogger(PartitionStatusOnlineInstanceFinder.class);
  private final OfflinePushAccessor offlinePushAccessor;
  private final RoutingDataRepository routingDataRepository;

  private final Map<String, List<PartitionStatus>> topicToPartitionMap;

  public PartitionStatusOnlineInstanceFinder(OfflinePushAccessor offlinePushAccessor,
      RoutingDataRepository routingDataRepository) {
    this.offlinePushAccessor = offlinePushAccessor;
    this.routingDataRepository = routingDataRepository;
    this.topicToPartitionMap = new HashMap<>();

    refresh();
  }

  @Override
  public synchronized void onPartitionStatusChange(String topic, ReadOnlyPartitionStatus partitionStatus) {
    if (!topicToPartitionMap.containsKey(topic) || routingDataRepository.getPartitionAssignments(topic) == null) {
      logger.info("Instance finder received unknown partition status notification. Topic: " + topic + ", Partition id: "
          + partitionStatus.getPartitionId()  + ". Will ignore.");
    } else {
      OfflinePushStatus.setPartitionStatus(topicToPartitionMap.get(topic), partitionStatus, topic,
          routingDataRepository.getPartitionAssignments(topic).getExpectedNumberOfPartitions());
    }
  }

  /**
   * TODO: check if we need to cache the result since this method is called very frequently.
   */
  @Override
  public List<Instance> getReadyToServeInstances(String kafkaTopic, int partitionId) {
    List<PartitionStatus> partitionStatusList = topicToPartitionMap.get(kafkaTopic);
    if (partitionStatusList == null) {
      //haven't received partition info related to this topic. Return empty list
      return Collections.emptyList();
    }

    return routingDataRepository.getAllInstances(kafkaTopic, partitionId).values().stream()
        .flatMap(List::stream)
        .filter(instance -> PushStatusDecider.getReplicaCurrentStatus(partitionStatusList
            .get(partitionId).getReplicaHistoricStatusList(instance.getNodeId()))
            .equals(ExecutionStatus.COMPLETED))
        .collect(Collectors.toList());
  }

  @Override
  public Map<String, List<Instance>> getAllInstances(String kafkaTopic, int partitionId) {
    return routingDataRepository.getAllInstances(kafkaTopic, partitionId);
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
      topicToPartitionMap.put(pushStatus.getKafkaTopic(), new ArrayList<>(pushStatus.getPartitionStatuses()));
      offlinePushAccessor.subscribePartitionStatusChange(pushStatus, this);
    });

    offlinePushAccessor.subscribePushStatusCreationChange(this);
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
        offlinePushAccessor.subscribePartitionStatusChange(status, this);
      }
    });

    deletedPushStatusList.forEach(pushStatusName -> topicToPartitionMap.remove(pushStatusName));
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
