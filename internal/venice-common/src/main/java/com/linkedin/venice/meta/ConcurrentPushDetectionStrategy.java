package com.linkedin.venice.meta;

public enum ConcurrentPushDetectionStrategy {
  TOPIC_BASED_ONLY, // Current mode using parent VT
  DUAL, // Read from parent zk status and Pubsub and compare
  PARENT_VERSION_STATUS_ONLY
}
