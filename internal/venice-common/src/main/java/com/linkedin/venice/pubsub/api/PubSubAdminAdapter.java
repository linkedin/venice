package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubInvalidReplicationFactorException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicExistsException;
import com.linkedin.venice.utils.RetryUtils;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;


/**
 * An adapter for PubSubAdmin to create/delete/list/update topics.
 */
public interface PubSubAdminAdapter extends Closeable {
  /**
   * Creates a new topic in the PubSub system with the given parameters.
   *
   * @param topicName The name of the topic to be created.
   * @param numPartitions The number of partitions to be created for the topic.
   * @param replication The number of replicas for each partition.
   * @param pubSubTopicConfiguration Additional topic configuration such as retention, compaction policy, etc.
   * @throws IllegalArgumentException If the replication factor is greater than Short.MAX_VALUE.
   * @throws PubSubInvalidReplicationFactorException If the provided replication factor is invalid according to broker constraints, or if the number of brokers available is less than the provided replication factor.
   * @throws PubSubTopicExistsException If a topic with the same name already exists.
   * @throws PubSubClientException For all other issues related to the PubSub client.
   *
   * @see PubSubTopic
   * @see PubSubTopicConfiguration
   * @see PubSubInvalidReplicationFactorException
   * @see PubSubTopicExistsException
   * @see PubSubClientException
   */
  void createTopic(
      PubSubTopic topicName,
      int numPartitions,
      int replication,
      PubSubTopicConfiguration pubSubTopicConfiguration);

  Future<Void> deleteTopic(PubSubTopic topicName);

  Set<PubSubTopic> listAllTopics();

  void setTopicConfig(PubSubTopic topicName, PubSubTopicConfiguration pubSubTopicConfiguration)
      throws PubSubTopicDoesNotExistException;

  Map<PubSubTopic, Long> getAllTopicRetentions();

  PubSubTopicConfiguration getTopicConfig(PubSubTopic topicName) throws PubSubTopicDoesNotExistException;

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

  String getClassName();

  Map<PubSubTopic, PubSubTopicConfiguration> getSomeTopicConfigs(Set<PubSubTopic> topicNames);

}
