package com.linkedin.venice.pushmonitor;

/**
 * This enum controls the behavior of how {@link PushMonitorDelegator} chooses
 * proper PushMonitor for resources.
 */
public enum PushMonitorType {
  // Resources belonging to write-compute enabled store are managed by
  // PartitionBasedPushStatusMonitor, the rest are managed by
  // OfflinePushMonitor
  WRITE_COMPUTE_STORE,

  // Resources belonging to hybrid enabled or write-compute enabled store are
  // managed by PartitionBasedPushStatusMonitor, the rest are managed
  // by OfflinePushMonitor
  HYBRID_STORE,

  // All resources are managed by PartitionStatusBasedPushMonitor
  PARTITION_STATUS_BASED,

  // L/F resources will be managed by CVBasedPushMonitor
  CV_PUSH_MONITOR_FOR_LF_STORE
}
