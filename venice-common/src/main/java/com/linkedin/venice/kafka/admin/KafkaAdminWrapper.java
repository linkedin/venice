package com.linkedin.venice.kafka.admin;

import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.utils.RetryUtils;
import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
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

  default boolean containsTopicWithRetry(String topic, int maxAttempts) {
    Duration initialBackoff = Duration.ofMillis(2);
    Duration maxBackoff = Duration.ofMillis(100);
    Duration maxDuration = Duration.ofSeconds(10);
    try {
      return RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
          () -> {
            if (!this.containsTopic(topic)) {
              throw new VeniceRetriableException("Couldn't get topic!  Retrying...");
            }
            return true;
          },
          maxAttempts,
          initialBackoff,
          maxBackoff,
          maxDuration,
          Collections.singletonList(VeniceRetriableException.class)
      );
    } catch (VeniceRetriableException e) {
      return false;
    }
  }

  Map<String, Properties> getAllTopicConfig();

  boolean isTopicDeletionUnderway();

  String getClassName();

  Map<String, KafkaFuture<TopicDescription>> describeTopics(Collection<String> topicNames);
}
