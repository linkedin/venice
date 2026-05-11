package com.linkedin.venice.controller.kafka;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubTopicType;
import java.util.ArrayList;
import java.util.List;


public class AdminTopicUtils {
  public static final int PARTITION_NUM_FOR_ADMIN_TOPIC = 1;
  public static final int ADMIN_TOPIC_PARTITION_ID = 0;

  /**
   * Maximum serialized size of a single admin-topic record (key + value bytes), enforced by the
   * controller before allocating an execution id.
   *
   * <p>Anchored at 950 KB to match the producer's rejection threshold
   * ({@code VeniceWriter#DEFAULT_MAX_SIZE_FOR_USER_PAYLOAD_PER_MESSAGE_IN_BYTES}). If the pre-flight
   * is looser than what the writer will accept, an oversized message gets past us, the producer
   * rejects it on {@code put}, and we leak the already-allocated execution id -- which is the bug
   * this guard exists to prevent.
   *
   * <p>It is also conservatively below the ZooKeeper {@code jute.maxbuffer} default of ~1 MB, so
   * admin operations that pass this check will fit in the eventual znode write performed by the
   * consumer.
   *
   * <p>The constant intentionally does not read from the writer's config: this keeps the pre-flight
   * independent of whether Venice chunking or pubsub-large-message passthrough is enabled on the
   * admin writer, both of which would otherwise route oversized records through alternate paths
   * that this guard cannot model precisely.
   */
  public static final int MAX_ADMIN_MESSAGE_PAYLOAD_SIZE_BYTES = 950 * 1024;

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
