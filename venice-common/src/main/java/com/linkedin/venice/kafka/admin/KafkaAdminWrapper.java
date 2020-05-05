package com.linkedin.venice.kafka.admin;

import java.io.Closeable;
import java.util.Map;
import java.util.Properties;


/**
 * In addition to the APIs below, implementers of this interface are expected to provide a public no-args constructor.
 */
public interface KafkaAdminWrapper extends Closeable {
  void initialize(Properties properties);

  void createTopic(String topicName, int numPartitions, int replication, Properties topicProperties);

  void deleteTopic(String topicName);

  void setTopicConfig(String topicName, Properties topicProperties);

  Map<String, Long> getAllTopicRetentions();

  Properties getTopicConfig(String topicName);

  boolean containsTopic(String topic);

  Map<String, Properties> getAllTopicConfig();

  boolean isTopicDeletionUnderway();
}
