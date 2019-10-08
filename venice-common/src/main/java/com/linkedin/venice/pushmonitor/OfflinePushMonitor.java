package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.utils.Pair;
import java.util.List;
import java.util.Optional;

/**
 * {@link OfflinePushStatus} watches the changes of {@link RoutingDataRepository}.
 * For batch push, push status is updated according to Helix OfflineOnline state
 * For streaming push, push status is updated according to partition status
 *
 * TODO: rename this class to HelixBasedPushStatusMonitor as we have move most of the code to {@link AbstractPushMonitor}
 */

public class OfflinePushMonitor extends AbstractPushMonitor implements RoutingDataRepository.RoutingDataChangedListener {
  public OfflinePushMonitor(String clusterName, RoutingDataRepository routingDataRepository,
      OfflinePushAccessor offlinePushAccessor, StoreCleaner storeCleaner, ReadWriteStoreRepository metadataRepository,
      AggPushHealthStats aggPushHealthStats, boolean skipBufferReplayForHybrid) {
    super(clusterName, offlinePushAccessor, storeCleaner, metadataRepository, routingDataRepository, aggPushHealthStats,
        skipBufferReplayForHybrid);
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
