package com.linkedin.venice.pubsub.api;

public interface PubSubTopic {
  /**
   * @return the name of the topic
   */
  String getName();

  /**
   * @return whether it is a RT topic
   */
  boolean isRealTimeTopic();

  /**
   * @return whether it is a version-topic.
   */
  boolean isVersionTopic();

  /**
   * @return whether it is a reprocessing topic.
   */
  boolean isReprocessingTopic();

  /**
   * @return the store name that this topic is associated with.
   */
  String getStoreName();
}
