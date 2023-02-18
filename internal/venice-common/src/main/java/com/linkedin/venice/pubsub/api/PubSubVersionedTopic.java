package com.linkedin.venice.pubsub.api;

public interface PubSubVersionedTopic extends PubSubTopic {
  /**
   * @return the version number that this topic is associated with
   */
  int getVersionNumber();
}
