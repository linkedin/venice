package com.linkedin.venice.kafka.admin;

import com.linkedin.venice.kafka.TopicDoesNotExistException;
import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;


/**
 * In addition to the APIs below, implementers of this interface are expected to provide a public no-args constructor.
 */
public interface KafkaAdminWrapper extends Closeable {
  void initialize(Properties properties);

  void createTopic(String topicName, int numPartitions, int replication, Properties topicProperties);

  KafkaFuture<Void> deleteTopic(String topicName);

  Set<String> listAllTopics();

  void setTopicConfig(String topicName, Properties topicProperties) throws TopicDoesNotExistException;

  Map<String, Long> getAllTopicRetentions();

  Properties getTopicConfig(String topicName) throws TopicDoesNotExistException;

  Properties getTopicConfigWithRetry(String topicName);

  boolean containsTopic(String topic);

  Map<String, Properties> getAllTopicConfig();

  boolean isTopicDeletionUnderway();

  String getClassName();

  Map<String, KafkaFuture<TopicDescription>> describeTopics(Collection<String> topicNames);
}
