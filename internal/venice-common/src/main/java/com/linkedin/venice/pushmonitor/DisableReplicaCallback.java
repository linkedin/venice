package com.linkedin.venice.pushmonitor;

/**
 * The callback method to disable leader replicas whenever they are in ERROR state so that helix
 * can elect a new leader.
 */
public interface DisableReplicaCallback {
  void disableReplica(String instance, int partitionId);

  boolean isReplicaDisabled(String instance, int partitionId);
}
