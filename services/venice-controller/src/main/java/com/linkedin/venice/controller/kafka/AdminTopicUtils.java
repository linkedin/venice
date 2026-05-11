package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubTopicType;
import com.linkedin.venice.writer.VeniceWriter;
import java.util.ArrayList;
import java.util.List;


public class AdminTopicUtils {
  public static final int PARTITION_NUM_FOR_ADMIN_TOPIC = 1;
  public static final int ADMIN_TOPIC_PARTITION_ID = 0;

  /**
   * Maximum serialized size of a single admin-topic record, enforced before allocating an
   * execution id. Aliased to {@link VeniceWriter#DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES}
   * to track the producer's default rejection threshold, and is conservatively below ZK's
   * {@code jute.maxbuffer} (~1 MB) so admin ops that pass this check will fit in the eventual znode.
   *
   * <p>Read as a compile-time constant rather than the writer's runtime
   * {@code getMaxSizeForUserPayloadPerMessageInBytes()} so the pre-flight stays independent of
   * whether Venice chunking or pubsub-large-message passthrough is enabled on the admin writer.
   */
  public static final int MAX_ADMIN_MESSAGE_PAYLOAD_SIZE_BYTES =
      VeniceWriter.DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES;

  private static final String KAFKA_INTERNAL_TOPIC_PREFIX = "__";
  private static final List<String> TOPICS_TO_IGNORE;
  static {
    TOPICS_TO_IGNORE = new ArrayList<>();
    TOPICS_TO_IGNORE.add("kafka-monitor-topic");
    TOPICS_TO_IGNORE.add("TrackingMonitoringEvent");
  }

  public static String getTopicNameFromClusterName(String clusterName) {
    return PubSubTopicType.ADMIN_TOPIC_PREFIX + clusterName;
  }

  public static String getClusterNameFromTopicName(String topicName) {
    if (!isAdminTopic(topicName)) {
      throw new VeniceException("Invalid admin topic name: " + topicName);
    }
    return topicName.substring(PubSubTopicType.ADMIN_TOPIC_PREFIX.length());
  }

  public static boolean isAdminTopic(String topicName) {
    return PubSubTopicType.isAdminTopic(topicName);
  }

  public static boolean isKafkaInternalTopic(String topicName) {
    return topicName.startsWith(KAFKA_INTERNAL_TOPIC_PREFIX) || TOPICS_TO_IGNORE.contains(topicName);
  }
}
