package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.ArrayList;
import java.util.List;


public class AdminTopicUtils {
  public static int PARTITION_NUM_FOR_ADMIN_TOPIC = 1;
  public static final int ADMIN_TOPIC_PARTITION_ID = 0;
  // TODO: Maybe we should make this prefix configurable so that different environments can share
  // the same Kafka cluster
  public static final String ADMIN_TOPIC_PREFIX = "venice_admin_";
  private static final String KAFKA_INTERNAL_TOPIC_PREFIX = "__";
  private static final List<String> TOPICS_TO_IGNORE;
  static {
    TOPICS_TO_IGNORE = new ArrayList<>();
    TOPICS_TO_IGNORE.add("kafka-monitor-topic");
    TOPICS_TO_IGNORE.add("TrackingMonitoringEvent");
  }

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

  public static boolean isKafkaInternalTopic(String topicName) {
    return topicName.startsWith(KAFKA_INTERNAL_TOPIC_PREFIX) || TOPICS_TO_IGNORE.contains(topicName);
  }
}
