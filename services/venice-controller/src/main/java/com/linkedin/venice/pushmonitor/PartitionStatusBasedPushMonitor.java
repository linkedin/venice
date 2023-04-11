package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.controller.HelixAdminClient;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.locks.ClusterLockManager;
import java.util.List;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * An implementation of {@link AbstractPushMonitor} that listens to ZK {@link PartitionStatus}
 * to determine the push status.
 */

public class PartitionStatusBasedPushMonitor extends AbstractPushMonitor {
  private static final Logger LOGGER = LogManager.getLogger(PartitionStatusBasedPushMonitor.class);

  public PartitionStatusBasedPushMonitor(
      String clusterName,
      OfflinePushAccessor offlinePushAccessor,
      StoreCleaner storeCleaner,
      ReadWriteStoreRepository metadataRepository,
      RoutingDataRepository routingDataRepository,
      AggPushHealthStats aggPushHealthStats,
      RealTimeTopicSwitcher realTimeTopicSwitcher,
      ClusterLockManager clusterLockManager,
      String aggregateRealTimeSourceKafkaUrl,
      List<String> childDataCenterKafkaUrls,
      HelixAdminClient helixAdminClient,
      boolean disableErrorLeaderReplica,
      long offlineJobResourceAssignmentWaitTimeInMilliseconds) {
    super(
        clusterName,
        offlinePushAccessor,
        storeCleaner,
        metadataRepository,
        routingDataRepository,
        aggPushHealthStats,
        realTimeTopicSwitcher,
        clusterLockManager,
        aggregateRealTimeSourceKafkaUrl,
        childDataCenterKafkaUrls,
        helixAdminClient,
        disableErrorLeaderReplica,
        offlineJobResourceAssignmentWaitTimeInMilliseconds);
  }

  @Override
  public void onPartitionStatusChange(OfflinePushStatus offlinePushStatus) {
    String kafkaTopic = offlinePushStatus.getKafkaTopic();
    /**
     * If the current push status is not terminal, we need the special check inside PartitionStatusBasedPushMonitor
     * to figure out whether the entire push job is terminal now after receiving one partition status change.
     */
    if (getRoutingDataRepository().containsKafkaTopic(kafkaTopic)) {
      boolean isTerminalStatus = offlinePushStatus.getCurrentStatus().isTerminal();
      if (!isTerminalStatus) {
        updatePushStatusByPartitionStatus(
            offlinePushStatus,
            getRoutingDataRepository().getPartitionAssignments(kafkaTopic));
      }

      super.onPartitionStatusChange(offlinePushStatus);
    }
  }

  private void updatePushStatusByPartitionStatus(
      OfflinePushStatus offlinePushStatus,
      PartitionAssignment partitionAssignment) {
    Pair<ExecutionStatus, Optional<String>> status = checkPushStatus(
        offlinePushStatus,
        partitionAssignment,
        getDisableReplicaCallback(partitionAssignment.getTopic()));
    if (status.getFirst().isTerminal()) {
      LOGGER.info(
          "Found a offline pushes could be terminated: {} status: {}",
          offlinePushStatus.getKafkaTopic(),
          status.getFirst());
      handleOfflinePushUpdate(offlinePushStatus, status.getFirst(), status.getSecond());
    }
  }

  /**
   * Checking push status based on Venice offlinePush status
   */
  @Override
  protected Pair<ExecutionStatus, Optional<String>> checkPushStatus(
      OfflinePushStatus pushStatus,
      PartitionAssignment partitionAssignment,
      DisableReplicaCallback callback) {
    return PushStatusDecider.getDecider(pushStatus.getStrategy())
        .checkPushStatusAndDetailsByPartitionsStatus(pushStatus, partitionAssignment, callback);
  }

  @Override
  public List<Instance> getReadyToServeInstances(PartitionAssignment partitionAssignment, int partitionId) {
    return PushStatusDecider.getReadyToServeInstances(
        getOfflinePushOrThrow(partitionAssignment.getTopic()).getPartitionStatus(partitionId),
        partitionAssignment,
        partitionId);
  }
}
