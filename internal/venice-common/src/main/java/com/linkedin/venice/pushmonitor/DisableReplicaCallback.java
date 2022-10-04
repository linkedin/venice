package com.linkedin.venice.pushmonitor;

public abstract class DisableReplicaCallback {
  public abstract void disableReplica(String instance, int partitionId);
}
