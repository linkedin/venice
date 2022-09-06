package com.linkedin.venice.pushmonitor;

/**
 * Listener used to listen the data change of partition status.
 */
public interface PartitionStatusListener {
  void onPartitionStatusChange(String topic, ReadOnlyPartitionStatus partitionStatus);
}
