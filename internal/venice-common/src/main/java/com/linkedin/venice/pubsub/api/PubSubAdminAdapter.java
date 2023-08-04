package com.linkedin.venice.pubsub.api;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS_DEFAULT_VALUE;
import static com.linkedin.venice.utils.Time.MS_PER_SECOND;

import com.linkedin.venice.exceptions.VeniceRetriableException;
import com.linkedin.venice.pubsub.PubSubTopicConfiguration;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubInvalidReplicationFactorException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicExistsException;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.Utils;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * An adapter for PubSubAdmin to create/delete/list/update topics.
 */
public interface PubSubAdminAdapter extends Closeable {
  /**
   * Creates a new topic in the PubSub system with the given parameters.
   *
   * @param pubSubTopic The topic to be created.
   * @param numPartitions The number of partitions to be created for the topic.
   * @param replicationFactor The number of replicas for each partition.
   * @param pubSubTopicConfiguration Additional topic configuration such as retention, compaction policy, etc.
   * @throws PubSubInvalidReplicationFactorException If the provided replication factor is invalid according to broker constraints, or if the number of brokers available is less than the provided replication factor.
   * @throws PubSubTopicExistsException If a topic with the same name already exists.
   * @throws PubSubClientRetriableException If the operation failed due to a retriable error.
   * @throws PubSubClientException For all other issues related to the PubSub client.
   *
   * @see PubSubTopic
   * @see PubSubTopicConfiguration
   * @see PubSubInvalidReplicationFactorException
   * @see PubSubTopicExistsException
   * @see PubSubClientRetriableException
   * @see PubSubClientException
   */
  void createTopic(
      PubSubTopic pubSubTopic,
      int numPartitions,
      int replicationFactor,
      PubSubTopicConfiguration pubSubTopicConfiguration);

  /**
   * Delete a given topic.
   * The calling thread will block until the topic is deleted or the timeout is reached.
   *
   * @param pubSubTopic The topic to delete.
   * @param timeout The maximum duration to wait for the deletion to complete.
   *
   * @throws PubSubTopicDoesNotExistException If the topic does not exist.
   * @throws PubSubOpTimeoutException If the operation times out.
   * @throws PubSubClientRetriableException If the operation fails and can be retried.
   * @throws PubSubClientException For all other issues related to the PubSub client.
   */
  void deleteTopic(PubSubTopic pubSubTopic, Duration timeout);

  /**
   * Retrieves the configuration of a PubSub topic.
   *
   * @param pubSubTopic The PubSubTopic for which to retrieve the configuration.
   * @return The configuration of the specified PubSubTopic as a PubSubTopicConfiguration object.
   *
   * @throws PubSubTopicDoesNotExistException If the specified PubSubTopic topic does not exist.
   * @throws PubSubClientRetriableException If a retriable error occurs while attempting to retrieve the configuration.
   * @throws PubSubClientException If an error occurs while attempting to retrieve the configuration or if the current thread is interrupted while attempting to retrieve the configuration.
   */
  PubSubTopicConfiguration getTopicConfig(PubSubTopic pubSubTopic);

  default PubSubTopicConfiguration getTopicConfigWithRetry(PubSubTopic pubSubTopic) {
    long accumulatedWaitTime = 0;
    long sleepIntervalInMs = 100;
    Exception exception = null;
    while (accumulatedWaitTime < getMaxGetTopicConfigRetryTimeInMs()) {
      try {
        return getTopicConfig(pubSubTopic);
      } catch (PubSubClientRetriableException | PubSubClientException e) {
        exception = e;
        Utils.sleep(sleepIntervalInMs);
        accumulatedWaitTime += sleepIntervalInMs;
        sleepIntervalInMs = Math.min(5 * MS_PER_SECOND, sleepIntervalInMs * 2);
      }
    }
    throw new PubSubClientException(
        "After retrying for " + accumulatedWaitTime + "ms, failed to get topic configs for: " + pubSubTopic,
        exception);
  }

  /**
   * Retrieves a set of all available PubSub topics from the PubSub cluster.
   *
   * @return A Set of PubSubTopic objects representing all available topics.
   *
   * @throws PubSubClientRetriableException If a retriable error occurs while attempting to retrieve the list of topics.
   * @throws PubSubClientException If an error occurs while attempting to retrieve the list of topics or the current thread is interrupted while attempting to retrieve the list of topics.
   */
  Set<PubSubTopic> listAllTopics();

  void setTopicConfig(PubSubTopic pubSubTopic, PubSubTopicConfiguration pubSubTopicConfiguration)
      throws PubSubTopicDoesNotExistException;

  Map<PubSubTopic, Long> getAllTopicRetentions();

  boolean containsTopic(PubSubTopic pubSubTopic);

  boolean containsTopicWithPartitionCheck(PubSubTopicPartition pubSubTopicPartition);

  /**
   * Retry up to a maximum number of attempts to get the expected result. If the topic existence check returns with
   * expected result, return the expected result immediately instead of retrying. This method exists since Kafka metadata
   * is eventually consistent so that it takes time for all Kafka brokers to learn about a topic creation takes. So checking
   * multiple times give us a more certain result of whether a topic exists.
   *
   * @param pubSubTopic
   * @param maxAttempts maximum number of attempts to check if no expected result is returned
   * @param expectedResult expected result
   * @return
   */
  default boolean containsTopicWithExpectationAndRetry(
      PubSubTopic pubSubTopic,
      int maxAttempts,
      final boolean expectedResult) {
    Duration defaultInitialBackoff = Duration.ofMillis(100);
    Duration defaultMaxBackoff = Duration.ofSeconds(5);
    Duration defaultMaxDuration = Duration.ofSeconds(60);
    return containsTopicWithExpectationAndRetry(
        pubSubTopic,
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
      PubSubTopic pubSubTopic,
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
        if (expectedResult != this.containsTopic(pubSubTopic)) {
          throw new VeniceRetriableException(
              "Retrying containsTopic check to get expected result: " + expectedResult + " for topic " + pubSubTopic);
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

  Map<PubSubTopic, PubSubTopicConfiguration> getSomeTopicConfigs(Set<PubSubTopic> pubSubTopics);

  // "admin.get.topic.config.max.retry.sec"
  default long getMaxGetTopicConfigRetryTimeInMs() {
    return Duration.ofSeconds(PUBSUB_ADMIN_GET_TOPIC_CONFIG_RETRY_IN_SECONDS_DEFAULT_VALUE).toMillis();
  }
}
