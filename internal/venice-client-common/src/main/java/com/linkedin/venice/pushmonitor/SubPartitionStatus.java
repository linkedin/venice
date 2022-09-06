package com.linkedin.venice.pushmonitor;

/**
 * SubPartitionStatus is an enum and is not related to { @link PartitionStatus }
 */
public enum SubPartitionStatus {
  STARTED, RESTARTED, END_OF_PUSH_RECEIVED, START_OF_BUFFER_REPLAY_RECEIVED, START_OF_INCREMENTAL_PUSH_RECEIVED,
  END_OF_INCREMENTAL_PUSH_RECEIVED, PROGRESS, TOPIC_SWITCH_RECEIVED, CATCH_UP_BASE_TOPIC_OFFSET_LAG, COMPLETED,
  DATA_RECOVERY_COMPLETED;
}
