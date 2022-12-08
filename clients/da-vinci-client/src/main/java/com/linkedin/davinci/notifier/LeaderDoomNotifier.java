package com.linkedin.davinci.notifier;

import static com.linkedin.venice.common.VeniceSystemStoreUtils.*;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.*;

import com.linkedin.venice.pushmonitor.OfflinePushAccessor;


public class LeaderDoomNotifier implements VeniceNotifier {
  private static boolean doOne = true;
  private final OfflinePushAccessor accessor;
  private final String instanceId;
  private static int count = 0;

  public LeaderDoomNotifier(OfflinePushAccessor accessor, String instanceId) {
    this.accessor = accessor;
    this.instanceId = instanceId;
  }

  @Override
  public void completed(String topic, int partitionId, long offset, String message) {
    if (doOne && message.equals("LEADER") && !isSystemStore(topic)) {
      accessor.updateReplicaStatus(topic, partitionId, instanceId, ERROR, message);
      doOne = false;
    } else {
      accessor.updateReplicaStatus(topic, partitionId, instanceId, COMPLETED, offset, "");
    }
  }
}
