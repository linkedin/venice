package com.linkedin.venice.pubsub.api;

public interface PubSubTopic {
  /**
   * @return the name of the topic
   */
  String getName();

  /**
   * @return the {@link PubSubTopicType} of the topic
   */
  PubSubTopicType getPubSubTopicType();

  /**
   * @return the store name that this topic is associated with.
   */
  String getStoreName();
}
