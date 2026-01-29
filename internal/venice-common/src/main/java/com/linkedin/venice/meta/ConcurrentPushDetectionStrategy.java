package com.linkedin.venice.meta;

public enum ConcurrentPushDetectionStrategy {
  TOPIC_BASED_ONLY(true), // Current mode using parent VT
  DUAL(true), // Read from parent zk status and Pubsub and compare
  PARENT_VERSION_STATUS_ONLY(false);

  private final boolean topicWriteNeeded;

  ConcurrentPushDetectionStrategy(boolean topicWriteNeeded) {
    this.topicWriteNeeded = topicWriteNeeded;
  }

  public boolean isTopicWriteNeeded() {
    return topicWriteNeeded;
  }
}
