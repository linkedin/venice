package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.views.VeniceView;


public enum PubSubTopicType {
  VERSION_TOPIC, REALTIME_TOPIC, REPROCESSING_TOPIC, ADMIN_TOPIC, VIEW_TOPIC, UNKNOWN_TYPE_TOPIC;

  public static final String ADMIN_TOPIC_PREFIX = "venice_admin_";

  public static PubSubTopicType getPubSubTopicType(String topicName) {
    if (Version.isRealTimeTopic(topicName)) {
      return REALTIME_TOPIC;
    } else if (Version.isStreamReprocessingTopic(topicName)) {
      return REPROCESSING_TOPIC;
    } else if (Version.isVersionTopic(topicName)) {
      return VERSION_TOPIC;
    } else if (isAdminTopic(topicName)) {
      return ADMIN_TOPIC;
    } else if (VeniceView.isViewTopic(topicName)) {
      return VIEW_TOPIC;
    } else {
      return UNKNOWN_TYPE_TOPIC;
    }
  }

  public static boolean isAdminTopic(String topicName) {
    return topicName.startsWith(PubSubTopicType.ADMIN_TOPIC_PREFIX);
  }
}
