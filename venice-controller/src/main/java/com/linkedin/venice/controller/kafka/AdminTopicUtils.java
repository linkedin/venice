package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.exceptions.VeniceException;

public class AdminTopicUtils {
  public static int PARTITION_NUM_FOR_ADMIN_TOPIC = 1;
  public static final int ADMIN_TOPIC_PARTITION_ID = 0;
  // TODO: Maybe we should make this prefix configurable so that different environments can share
  // the same Kafka cluster
  public static String ADMIN_TOPIC_PREFIX = "venice_admin_";

  public static String getTopicNameFromClusterName(String clusterName) {
    return ADMIN_TOPIC_PREFIX + clusterName;
  }

  public static String getClusterNameFromTopicName(String topicName) {
    if (!isAdminTopic(topicName)) {
      throw new VeniceException("Invalid admin topic name: " + topicName);
    }
    return topicName.substring(ADMIN_TOPIC_PREFIX.length());
  }

  public static boolean isAdminTopic(String topicName) {
    return topicName.startsWith(ADMIN_TOPIC_PREFIX);
  }
}
