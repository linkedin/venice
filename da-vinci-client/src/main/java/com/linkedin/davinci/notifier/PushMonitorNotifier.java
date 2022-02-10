package com.linkedin.davinci.notifier;

import com.linkedin.venice.pushmonitor.ExecutionStatus;
import com.linkedin.venice.pushmonitor.OfflinePushAccessor;


/**
 * Notifier used to update replica status by offline push monitor accessor.
 */
public class PushMonitorNotifier implements VeniceNotifier {

  private final OfflinePushAccessor accessor;
  private final OfflinePushAccessor pushStatusStoreAccessor;
  private final String instanceId;

  public PushMonitorNotifier(OfflinePushAccessor accessor, OfflinePushAccessor pushStatusStoreAccessor, String instanceId) {
    this.accessor = accessor;
    this.pushStatusStoreAccessor = pushStatusStoreAccessor;
    this.instanceId = instanceId;
  }

  @Override
  public void started(String topic, int partitionId, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, ExecutionStatus.STARTED, "");
  }

  @Override
  public void restarted(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, ExecutionStatus.STARTED, offset, "");
  }

  @Override
  public void completed(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, ExecutionStatus.COMPLETED, offset, "");
  }

  @Override
  public void progress(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, ExecutionStatus.PROGRESS, offset, "");
  }

  @Override
  public void endOfPushReceived(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, ExecutionStatus.END_OF_PUSH_RECEIVED, offset, "");
  }

  @Override
  public void startOfBufferReplayReceived(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, ExecutionStatus.START_OF_BUFFER_REPLAY_RECEIVED, offset, "");
  }

  @Override
  public void topicSwitchReceived(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, ExecutionStatus.TOPIC_SWITCH_RECEIVED, offset, "");
  }

  @Override
  public void startOfIncrementalPushReceived(String topic, int partitionId, long offset, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED, offset, message);
    pushStatusStoreAccessor.updateReplicaStatus(topic, partitionId, instanceId, ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED, offset, message);
  }

  @Override
  public void endOfIncrementalPushReceived(String topic, int partitionId, long offset, long highWatermark, String message) {
    accessor.updateReplicaStatus(topic, partitionId, instanceId, ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED, offset, message);
    pushStatusStoreAccessor.updateReplicaHighWatermarkStatus(topic, partitionId, instanceId, highWatermark, message);
  }

  @Override
  public void close() {
    // Do not need to close here. accessor should be closed by the outer class.
  }

  @Override
  public void error(String topic, int partitionId, String message, Exception ex) {
    //TODO record error message as well.
    accessor.updateReplicaStatus(topic, partitionId, instanceId, ExecutionStatus.ERROR, message);
  }
}
