package com.linkedin.venice.meta;

import com.linkedin.venice.pushmonitor.PartitionStatusOnlineInstanceFinder;
import com.linkedin.venice.routerapi.ReplicaState;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.log4j.Logger;


public class OnlineInstanceFinderDelegator implements OnlineInstanceFinder {
  private static final Logger logger = Logger.getLogger(OnlineInstanceFinderDelegator.class);

  private final ReadOnlyStoreRepository metadataRepo;
  private final RoutingDataRepository routingDataOnlineInstanceFinder;
  private final PartitionStatusOnlineInstanceFinder partitionStatusOnlineInstanceFinder;

  private final Map<String, OnlineInstanceFinder> topicToInstanceFinderMap = new VeniceConcurrentHashMap<>();

  public OnlineInstanceFinderDelegator(ReadOnlyStoreRepository metadataRepo, RoutingDataRepository routingDataOnlineInstanceFinder,
      PartitionStatusOnlineInstanceFinder partitionStatusOnlineInstanceFinder) {
    this.metadataRepo = metadataRepo;
    this.routingDataOnlineInstanceFinder = routingDataOnlineInstanceFinder;
    this.partitionStatusOnlineInstanceFinder = partitionStatusOnlineInstanceFinder;
  }

  @Override
  public List<Instance> getReadyToServeInstances(String kafkaTopic, int partitionId) {
    return getInstanceFinder(kafkaTopic).getReadyToServeInstances(kafkaTopic, partitionId);
  }

  @Override
  public Map<String, List<Instance>> getAllInstances(String kafkaTopic, int partitionId) {
    return getInstanceFinder(kafkaTopic).getAllInstances(kafkaTopic, partitionId);
  }

  @Override
  public List<ReplicaState> getReplicaStates(String kafkaTopic, int partitionId) {
    return getInstanceFinder(kafkaTopic).getReplicaStates(kafkaTopic, partitionId);
  }

  @Override
  public int getNumberOfPartitions(String kafkaTopic) {
    return getInstanceFinder(kafkaTopic).getNumberOfPartitions(kafkaTopic);
  }

  private OnlineInstanceFinder getInstanceFinder(String kafkaTopic) {
    return topicToInstanceFinderMap.computeIfAbsent(kafkaTopic, topic -> {
      Store store = metadataRepo.getStore(Version.parseStoreFromKafkaTopicName(kafkaTopic));
      if (store == null) {
        logger.warn("Cannot find store corresponding to the topic. Use partition status based instance finder by default."
            + " Topic: " + kafkaTopic);
        return partitionStatusOnlineInstanceFinder;
      }

      Optional<Version> version = store.getVersion(Version.parseVersionFromKafkaTopicName(kafkaTopic));
      if (!version.isPresent()) {
        logger.warn("Version finder cannot retrieve version info from store metadata repo. Use store's metadata by default."
            + " Store: " + store.getName() + " Version: " + version);
        return store.isLeaderFollowerModelEnabled() ? partitionStatusOnlineInstanceFinder : routingDataOnlineInstanceFinder;
      }

      return version.get().isLeaderFollowerModelEnabled() ? partitionStatusOnlineInstanceFinder : routingDataOnlineInstanceFinder;
    });
  }
}
