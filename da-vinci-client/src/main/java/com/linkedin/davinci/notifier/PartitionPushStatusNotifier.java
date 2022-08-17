package com.linkedin.davinci.notifier;

import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.HybridStoreQuotaStatus;


/**
 * Notifier used to update replica status by Helix partition push status accessor.
 */
public class PartitionPushStatusNotifier implements VeniceNotifier {
  private HelixPartitionStatusAccessor accessor;

  public PartitionPushStatusNotifier(HelixPartitionStatusAccessor accessor) {
    this.accessor = accessor;
  }

  @Override
  public void started(String topic, int partitionId, String message) {
    accessor.updateReplicaStatus(topic, partitionId, ExecutionStatus.STARTED);
  }

  @Override
  public void restarted(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, ExecutionStatus.STARTED);
  }

  @Override
  public void completed(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, ExecutionStatus.COMPLETED);
  }

  @Override
  public void progress(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, ExecutionStatus.PROGRESS);
  }

  @Override
  public void quotaViolated(String topic, int partitionId, long offset, String message) {
    accessor.updateHybridQuotaReplicaStatus(topic, partitionId, HybridStoreQuotaStatus.QUOTA_VIOLATED);
  }

  @Override
  public void quotaNotViolated(String topic, int partitionId, long offset, String message) {
    accessor.updateHybridQuotaReplicaStatus(topic, partitionId, HybridStoreQuotaStatus.QUOTA_NOT_VIOLATED);
  }

  @Override
  public void close() {
    // Do not need to close here. accessor should be closed by the outer class.
  }

  @Override
  public void error(String topic, int partitionId, String message, Exception ex) {
    accessor.updateReplicaStatus(topic, partitionId, ExecutionStatus.ERROR);
  }
}
