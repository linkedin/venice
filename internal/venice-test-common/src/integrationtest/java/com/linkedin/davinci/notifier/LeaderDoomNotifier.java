package com.linkedin.davinci.notifier;

import static com.linkedin.venice.common.VeniceSystemStoreUtils.*;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.*;

import com.linkedin.venice.pushmonitor.OfflinePushAccessor;


/**
 * A test only notifier to simulate ERROR in leader replica to test single leader replica failover scenario.
 */
public class LeaderDoomNotifier implements VeniceNotifier {
  private boolean doOne = true;
  private final OfflinePushAccessor accessor;
  private final String instanceId;

  public LeaderDoomNotifier(OfflinePushAccessor accessor, String instanceId) {
    this.accessor = accessor;
    this.instanceId = instanceId;
  }

  @Override
  public void completed(String topic, int partitionId, long offset, String message) {
    if (doOne && message.contains("LEADER") && !isSystemStore(topic)) {
      accessor.updateReplicaStatus(topic, partitionId, instanceId, ERROR, "");
      doOne = false;
    } else {
      accessor.updateReplicaStatus(topic, partitionId, instanceId, COMPLETED, offset, "");
    }
  }
}
