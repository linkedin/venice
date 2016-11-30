package com.linkedin.venice.notifier;

import com.linkedin.venice.job.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushMonitorAccessor;


/**
 * Notifier used to update replica status by offline push monitor accessor.
 */
public class PushMonitorNotifier implements VeniceNotifier {

  private OfflinePushMonitorAccessor accessor;
  private String instanceId;

  public PushMonitorNotifier(OfflinePushMonitorAccessor accessor, String instanceId) {
    this.accessor = accessor;
    this.instanceId = instanceId;
  }

  @Override
  public void started(String topic, int partitionId) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, ExecutionStatus.STARTED);
  }

  @Override
  public void restarted(String topic, int partitionId, long offset) {
    // Because we didn't remove status for this replica, so we don't need to update it while restarting.
  }

  @Override
  public void completed(String topic, int partitionId, long offset) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, ExecutionStatus.COMPLETED, offset);
  }

  @Override
  public void progress(String topic, int partitionId, long offset) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, ExecutionStatus.PROGRESS, offset);
  }

  @Override
  public void close() {
    // Do not need to close here. accessor should be closed by the outer class.
  }

  @Override
  public void error(String topic, int partitionId, String message, Exception ex) {
    //TODO record error message as well.
    accessor.updateReplicaStatus(topic, partitionId, instanceId, ExecutionStatus.ERROR);
  }
}
