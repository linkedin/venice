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

  default boolean isRealTime() {
    return getPubSubTopicType() == PubSubTopicType.REALTIME_TOPIC;
  }

  default boolean isStreamReprocessingTopic() {
    return getPubSubTopicType() == PubSubTopicType.REPROCESSING_TOPIC;
  }

  default boolean isVersionTopic() {
    return getPubSubTopicType() == PubSubTopicType.VERSION_TOPIC;
  }

  default boolean isVersionTopicOrStreamReprocessingTopic() {
    return isStreamReprocessingTopic() || isVersionTopic();
  }

  default boolean isViewTopic() {
    return getPubSubTopicType() == PubSubTopicType.VIEW_TOPIC;
  }
}
