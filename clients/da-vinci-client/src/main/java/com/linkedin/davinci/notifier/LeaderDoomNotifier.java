package com.linkedin.davinci.notifier;

import static com.linkedin.venice.common.VeniceSystemStoreUtils.*;

import com.linkedin.venice.helix.HelixPartitionStatusAccessor;
import com.linkedin.venice.pushmonitor.ExecutionStatus;


public class LeaderDoomNotifier implements VeniceNotifier {
  private static boolean doOne = true;
  private final HelixPartitionStatusAccessor accessor;

  public LeaderDoomNotifier(HelixPartitionStatusAccessor accessor) {
    this.accessor = accessor;
  }

  @Override
  public void completed(String topic, int partitionId, long offset, String message) {
    if (doOne && message.equals("LEADER") && isSystemStore(topic)) {
      accessor.updateReplicaStatus(topic, partitionId, ExecutionStatus.ERROR);
      doOne = false;
    } else {
      accessor.updateReplicaStatus(topic, partitionId, ExecutionStatus.COMPLETED);
    }
  }
}
