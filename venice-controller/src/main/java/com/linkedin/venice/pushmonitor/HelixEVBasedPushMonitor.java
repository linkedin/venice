package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.List;
import java.util.Optional;

/**
 * An implementation of {@link AbstractPushMonitor} that listens to Helix External
 * View to determine the push status.
 *
 * This monitor can be only used for O/O resources.
 */

public class HelixEVBasedPushMonitor extends AbstractPushMonitor implements RoutingDataRepository.RoutingDataChangedListener {
  public HelixEVBasedPushMonitor(String clusterName, RoutingDataRepository routingDataRepository,
      OfflinePushAccessor offlinePushAccessor, StoreCleaner storeCleaner, ReadWriteStoreRepository metadataRepository,
      AggPushHealthStats aggPushHealthStats, Optional<TopicReplicator> topicReplicator,
      ClusterLockManager clusterLockManager, String aggregateRealTimeSourceKafkaUrl,
      List<String> activeActiveRealTimeSourceKafkaURLs) {
    super(clusterName, offlinePushAccessor, storeCleaner, metadataRepository, routingDataRepository, aggPushHealthStats,
        topicReplicator, clusterLockManager, aggregateRealTimeSourceKafkaUrl,
        activeActiveRealTimeSourceKafkaURLs);
  }
  /**
   * Checking push status based on Helix external view (RoutingData)
   */
  @Override
  protected Pair<ExecutionStatus, Optional<String>> checkPushStatus(OfflinePushStatus pushStatus, PartitionAssignment partitionAssignment) {
    PushStatusDecider statusDecider = PushStatusDecider.getDecider(pushStatus.getStrategy());
    return statusDecider.checkPushStatusAndDetails(pushStatus, partitionAssignment);
  }

  @Override
  public List<Instance> getReadyToServeInstances(PartitionAssignment partitionAssignment, int partitionId) {
    return getRoutingDataRepository().getReadyToServeInstances(partitionAssignment, partitionId);
  }
}
