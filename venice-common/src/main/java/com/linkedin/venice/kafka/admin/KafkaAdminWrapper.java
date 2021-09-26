package com.linkedin.venice.kafka.admin;

import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.utils.RetryUtils;
import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TimeoutException;


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

  boolean containsTopicWithPartitionCheck(String topic, int partitionID);

  /**
   * Retry up to a maximum number of attempts to get the expected result. If the topic existence check returns with
   * expected result, return the expected result immediately instead of retrying. This method exists since Kafka metadata
   * is eventually consistent so that it takes time for all Kafka brokers to learn about a topic creation takes. So checking
   * multiple times give us a more certain result of whether a topic exists.
   *
   * @param topic
   * @param maxAttempts maximum number of attempts to check if no expected result is returned
   * @param expectedResult expected result
   * @return
   */
  default boolean containsTopicWithExpectationAndRetry(String topic, int maxAttempts, final boolean expectedResult) {
    Duration defaultInitialBackoff = Duration.ofMillis(100);
    Duration defaultMaxBackoff = Duration.ofSeconds(5);
    Duration defaultMaxDuration = Duration.ofSeconds(60);
    return containsTopicWithExpectationAndRetry(
        topic,
        maxAttempts,
        expectedResult,
        defaultInitialBackoff,
        defaultMaxBackoff,
        defaultMaxDuration
    );
  }

  default boolean containsTopicWithPartitionCheckExpectationAndRetry(String topic, int partition, int maxAttempts, final boolean expectedResult) {
    Duration defaultInitialBackoff = Duration.ofMillis(100);
    Duration defaultMaxBackoff = Duration.ofSeconds(5);
    Duration defaultMaxDuration = Duration.ofSeconds(60);
    return containsTopicWithPartitionCheckExpectationAndRetry(
        topic,
        partition,
        maxAttempts,
        expectedResult,
        defaultInitialBackoff,
        defaultMaxBackoff,
        defaultMaxDuration
    );
  }

  List<Class<? extends Throwable>> RETRIABLE_EXCEPTIONS = Collections.unmodifiableList(Arrays.asList(
      VeniceRetriableException.class,
      TimeoutException.class));

  default boolean containsTopicWithExpectationAndRetry(
      String topic,
      int maxAttempts,
      final boolean expectedResult,
      Duration initialBackoff,
      Duration maxBackoff,
      Duration maxDuration
  ) {
    if (initialBackoff.toMillis() > maxBackoff.toMillis()) {
      throw new IllegalArgumentException("Initial backoff cannot be longer than max backoff. Got initial backoff in "
          + "millis: " + initialBackoff.toMillis() + " and max backoff in mills: " + maxBackoff.toMillis());
    }

    try {
      return RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
          () -> {
            if (expectedResult != this.containsTopic(topic)) {
              throw new VeniceRetriableException("Retrying containsTopic check to get expected result: " + expectedResult +
                  " for topic " + topic);
            }
            return expectedResult;
          },
          maxAttempts,
          initialBackoff,
          maxBackoff,
          maxDuration,
          RETRIABLE_EXCEPTIONS
      );
    } catch (VeniceRetriableException e) {
      return !expectedResult; // Eventually still not get the expected result
    }
  }

  default boolean containsTopicWithPartitionCheckExpectationAndRetry(
      String topic,
      int partition,
      int maxAttempts,
      final boolean expectedResult,
      Duration initialBackoff,
      Duration maxBackoff,
      Duration maxDuration
  ) {
    if (initialBackoff.toMillis() > maxBackoff.toMillis()) {
      throw new IllegalArgumentException("Initial backoff cannot be longer than max backoff. Got initial backoff in "
          + "millis: " + initialBackoff.toMillis() + " and max backoff in mills: " + maxBackoff.toMillis());
    }

    try {
      return RetryUtils.executeWithMaxAttemptAndExponentialBackoff(
          () -> {
            if (expectedResult != this.containsTopicWithPartitionCheck(topic, partition)) {
              throw new VeniceRetriableException("Retrying containsTopic check to get expected result: " + expectedResult +
                  " for topic " + topic);
            }
            return expectedResult;
          },
          maxAttempts,
          initialBackoff,
          maxBackoff,
          maxDuration,
          RETRIABLE_EXCEPTIONS
      );
    } catch (VeniceRetriableException e) {
      return !expectedResult; // Eventually still not get the expected result
    }
  }

  Map<String, Properties> getSomeTopicConfigs(Set<String> topicNames);

  boolean isTopicDeletionUnderway();

  String getClassName();

  Map<String, KafkaFuture<TopicDescription>> describeTopics(Collection<String> topicNames);
}
