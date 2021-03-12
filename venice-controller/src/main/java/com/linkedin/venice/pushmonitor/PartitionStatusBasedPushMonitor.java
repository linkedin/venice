package com.linkedin.venice.pushmonitor;

import com.linkedin.venice.controller.MetadataStoreWriter;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.ReadWriteStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.replication.TopicReplicator;
import com.linkedin.venice.utils.Pair;
import io.tehuti.metrics.MetricsRepository;
import java.util.List;
import java.util.Optional;

/**
 * Push status monitor based on {@link PartitionStatus} stored in ZK
 */

public class PartitionStatusBasedPushMonitor extends AbstractPushMonitor {
  public PartitionStatusBasedPushMonitor(String clusterName, OfflinePushAccessor offlinePushAccessor,
      StoreCleaner storeCleaner, ReadWriteStoreRepository metadataRepository, RoutingDataRepository routingDataRepository,
      AggPushHealthStats aggPushHealthStats, boolean skipBufferReplayForHybrid, Optional<TopicReplicator> topicReplicator,
      MetricsRepository metricsRepository, MetadataStoreWriter metadataStoreWriter) {
    super(clusterName, offlinePushAccessor, storeCleaner, metadataRepository, routingDataRepository, aggPushHealthStats,
        skipBufferReplayForHybrid, topicReplicator, metricsRepository, metadataStoreWriter);
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
        updatePushStatusByPartitionStatus(offlinePushStatus, getRoutingDataRepository().getPartitionAssignments(kafkaTopic));
      }

      super.onPartitionStatusChange(offlinePushStatus);
    }
  }

  private void updatePushStatusByPartitionStatus(OfflinePushStatus offlinePushStatus, PartitionAssignment partitionAssignment) {
    Pair<ExecutionStatus, Optional<String>> status = checkPushStatus(offlinePushStatus, partitionAssignment);
    if (status.getFirst().isTerminal()) {
      logger.info("Found a offline pushes could be terminated: " + offlinePushStatus.getKafkaTopic() + " status: " + status.getFirst());
      handleOfflinePushUpdate(offlinePushStatus, status.getFirst(), status.getSecond());
    }
  }

  /**
   * Checking push status based on Venice offlinePush status
   */
  @Override
  protected Pair<ExecutionStatus, Optional<String>> checkPushStatus(OfflinePushStatus pushStatus, PartitionAssignment partitionAssignment) {
    return PushStatusDecider.getDecider(pushStatus.getStrategy())
           .checkPushStatusAndDetailsByPartitionsStatus(pushStatus, partitionAssignment);
  }

  @Override
  public List<Instance> getReadyToServeInstances(PartitionAssignment partitionAssignment, int partitionId) {
    return PushStatusDecider.getReadyToServeInstances(
        getOfflinePushOrThrow(partitionAssignment.getTopic()).getPartitionStatus(partitionId), partitionAssignment, partitionId);
  }
}
