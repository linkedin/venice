package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;


public enum PubSubTopicType {
  VERSION_TOPIC, REALTIME_TOPIC, REPROCESSING_TOPIC;

  public static PubSubTopicType getPubSubTopicType(String topicName) {
    if (Version.isRealTimeTopic(topicName)) {
      return REALTIME_TOPIC;
    } else if (Version.isStreamReprocessingTopic(topicName)) {
      return REPROCESSING_TOPIC;
    } else if (Version.isVersionTopic(topicName)) {
      return VERSION_TOPIC;
    } else {
      throw new VeniceException("Unsupported topic type for: " + topicName);
    }
  }
}
