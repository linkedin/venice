package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.kafka.TopicDoesNotExistException;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.utils.RetryUtils;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;


/**
 * In addition to the APIs below, implementers of this interface are expected to provide a public no-args constructor.
 */
public interface PubSubAdminAdapter extends Closeable {
  void createTopic(
      PubSubTopic topicName,
      int numPartitions,
      int replication,
      PubSubTopicConfiguration pubSubTopicConfiguration);

  Future<Void> deleteTopic(PubSubTopic topicName);

  Set<PubSubTopic> listAllTopics();

  void setTopicConfig(PubSubTopic topicName, PubSubTopicConfiguration pubSubTopicConfiguration)
      throws TopicDoesNotExistException;

  Map<PubSubTopic, Long> getAllTopicRetentions();

  PubSubTopicConfiguration getTopicConfig(PubSubTopic topicName) throws TopicDoesNotExistException;

  PubSubTopicConfiguration getTopicConfigWithRetry(PubSubTopic topicName);

  boolean containsTopic(PubSubTopic topic);

  boolean containsTopicWithPartitionCheck(PubSubTopicPartition pubSubTopicPartition);

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
  default boolean containsTopicWithExpectationAndRetry(
      PubSubTopic topic,
      int maxAttempts,
      final boolean expectedResult) {
    Duration defaultInitialBackoff = Duration.ofMillis(100);
    Duration defaultMaxBackoff = Duration.ofSeconds(5);
    Duration defaultMaxDuration = Duration.ofSeconds(60);
    return containsTopicWithExpectationAndRetry(
        topic,
        maxAttempts,
        expectedResult,
        defaultInitialBackoff,
        defaultMaxBackoff,
        defaultMaxDuration);
  }

  default boolean containsTopicWithPartitionCheckExpectationAndRetry(
      PubSubTopicPartition pubSubTopicPartition,
      int maxAttempts,
      final boolean expectedResult) {
    Duration defaultAttemptDuration = Duration.ofSeconds(60);
    return containsTopicWithPartitionCheckExpectationAndRetry(
        pubSubTopicPartition,
        maxAttempts,
        expectedResult,
        defaultAttemptDuration);
  }

  List<Class<? extends Throwable>> getRetriableExceptions();

  default boolean containsTopicWithExpectationAndRetry(
      PubSubTopic topic,
      int maxAttempts,
      final boolean expectedResult,
      Duration initialBackoff,
      Duration maxBackoff,
      Duration maxDuration) {
    if (initialBackoff.toMillis() > maxBackoff.toMillis()) {
      throw new IllegalArgumentException(
          "Initial backoff cannot be longer than max backoff. Got initial backoff in " + "millis: "
              + initialBackoff.toMillis() + " and max backoff in mills: " + maxBackoff.toMillis());
    }

    try {
      return RetryUtils.executeWithMaxAttemptAndExponentialBackoff(() -> {
        if (expectedResult != this.containsTopic(topic)) {
          throw new VeniceRetriableException(
              "Retrying containsTopic check to get expected result: " + expectedResult + " for topic " + topic);
        }
        return expectedResult;
      }, maxAttempts, initialBackoff, maxBackoff, maxDuration, getRetriableExceptions());
    } catch (VeniceRetriableException e) {
      return !expectedResult; // Eventually still not get the expected result
    }
  }

  default boolean containsTopicWithPartitionCheckExpectationAndRetry(
      PubSubTopicPartition pubSubTopicPartition,
      int maxAttempts,
      final boolean expectedResult,
      Duration attemptDuration) {

    try {
      return RetryUtils.executeWithMaxRetriesAndFixedAttemptDuration(() -> {
        if (expectedResult != this.containsTopicWithPartitionCheck(pubSubTopicPartition)) {
          throw new VeniceRetriableException(
              "Retrying containsTopic check to get expected result: " + expectedResult + " for :"
                  + pubSubTopicPartition);
        }
        return expectedResult;
      }, maxAttempts, attemptDuration, getRetriableExceptions());
    } catch (VeniceRetriableException e) {
      return !expectedResult; // Eventually still not get the expected result
    }
  }

  boolean isTopicDeletionUnderway();

  String getClassName();

  Map<PubSubTopic, PubSubTopicConfiguration> getSomeTopicConfigs(Set<PubSubTopic> topicNames);

}
