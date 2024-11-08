package com.linkedin.venice.pubsub.api;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubUnsubscribedTopicPartitionException;
import java.io.Closeable;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * An adapter for consuming messages from a Pub-Sub topic.
 * Implementations of this interface are not expected to be thread safe. However, they are expected
 * to provide the following guarantees:
 * 1) Honor the timeout parameter for all methods that have one.
 * 2) Non-blocking behavior for methods that do not have an explicit timeout parameter. In other words, they should
 *  timeout after the default timeout period: {@link PubSubConstants#PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS}.
 */
public interface PubSubConsumerAdapter extends AutoCloseable, Closeable {
  /**
   * Subscribes to a topic-partition if it is not already subscribed. If the topic-partition is already subscribed,
   * this method is a no-op. The method assumes that the topic-partition exists.
   *
   * @param pubSubTopicPartition The topic-partition to subscribe to.
   * @param lastReadOffset The last read offset for the topic-partition. A poll call following a subscribe call
   *                      will return messages from the offset (lastReadOffset + 1).
   * @throws IllegalArgumentException If the topic-partition is null or if the partition number is negative.
   * @throws PubSubTopicDoesNotExistException If the topic does not exist.
   */
  void subscribe(PubSubTopicPartition pubSubTopicPartition, long lastReadOffset);

  /**
   * Unsubscribes the consumer from a specified topic-partition.
   * If the consumer was previously subscribed to the given partition, it will be unsubscribed,
   * and the associated partition assignments and tracked offsets will be updated accordingly.
   *
   * @param pubSubTopicPartition The PubSub topic-partition to unsubscribe from.
   */
  void unSubscribe(PubSubTopicPartition pubSubTopicPartition);

  /**
   * Unsubscribes the consumer from a batch of topic-partitions.
   *
   * @param pubSubTopicPartitionSet A set of topic-partitions to unsubscribe from.
   */
  void batchUnsubscribe(Set<PubSubTopicPartition> pubSubTopicPartitionSet);

  /**
   * Resets the offset for a specific topic-partition, seeking it to the beginning of the topic.
   * If the topic partition is not currently subscribed, a {@link PubSubUnsubscribedTopicPartitionException} is thrown.
   *
   * @param pubSubTopicPartition The PubSub topic-partition for which to reset the offset.
   * @throws PubSubUnsubscribedTopicPartitionException If the specified topic-partition is not currently subscribed.
   */
  void resetOffset(PubSubTopicPartition pubSubTopicPartition) throws PubSubUnsubscribedTopicPartitionException;

  /**
   * Closes the PubSub consumer and releases any associated resources.
   */
  void close();

  /**
   * Polls the Kafka consumer for messages from the subscribed topic partitions within the specified time duration.
   *
   * @param timeoutMs The maximum time, in milliseconds, to wait for messages to be polled.
   * @return A mapping of PubSub topic partitions to lists of PubSub messages retrieved from Kafka.
   * @throws PubSubClientException If there is an error during message retrieval from Kafka.
   * @throws PubSubClientRetriableException If a retriable exception occurs during polling attempts, with retries as configured.
   */
  Map<PubSubTopicPartition, List<PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long>>> poll(long timeoutMs);

  /**
   * Checks if the consumer has any active topic-partition subscriptions.
   *
   * @return true if the consumer is subscribed to one or more topic partitions, false otherwise.
   */
  boolean hasAnySubscription();

  /**
   * Checks if the consumer is currently subscribed to the specified PubSub topic-partition.
   *
   * @param pubSubTopicPartition The PubSub topic-partition to check for subscription.
   * @return true if the consumer is subscribed to the given topic-partition, false otherwise.
   */
  boolean hasSubscription(PubSubTopicPartition pubSubTopicPartition);

  /**
   * Pauses message consumption for the specified PubSub topic-partition. If the partition was not
   * previously subscribed, this method is a no-op.
   *
   * @param pubSubTopicPartition The PubSub topic-partition for which to pause message consumption.
   */
  void pause(PubSubTopicPartition pubSubTopicPartition);

  /**
   * Resumes message consumption for the specified PubSub topic-partition. If the partition was not
   * previously paused or if they were not subscribed at all, this method is a no-op.
   *
   * @param pubSubTopicPartition The PubSub topic partition for which to resume message consumption.
   */
  void resume(PubSubTopicPartition pubSubTopicPartition);

  /**
   * Retrieves the set of PubSub topic-partitions currently assigned to the consumer.
   *
   * @return A set of PubSub topic-partitions representing the current assignment of the consumer.
   */
  Set<PubSubTopicPartition> getAssignment();

  /**
   * Retrieves the consuming offset lag for a PubSub topic partition. The offset lag represents the difference
   * between the last consumed message offset and the latest available message offset for the partition.
   *
   * @param pubSubTopicPartition The PubSub topic partition for which to fetch the offset lag.
   * @return The offset lag, which is zero or a positive value if a valid lag was collected by the consumer,
   *         or -1 if the lag cannot be determined or is not applicable.
   */
  default long getOffsetLag(PubSubTopicPartition pubSubTopicPartition) {
    return -1;
  }

  /**
   * Retrieves the latest available offset for a PubSub topic partition.
   *
   * @param pubSubTopicPartition The PubSub topic partition for which to fetch the latest offset.
   * @return The latest offset, which is zero or a positive value if an offset was collected by the consumer,
   *         or -1 if the offset cannot be determined or is not applicable.
   */
  default long getLatestOffset(PubSubTopicPartition pubSubTopicPartition) {
    return -1;
  }

  /**
   * Retrieves the offset of the first message with a timestamp greater than or equal to the target
   * timestamp for the specified PubSub topic-partition. If no such message is found, {@code null}
   * will be returned for the partition.
   *
   * @param pubSubTopicPartition The PubSub topic-partition for which to fetch the offset.
   * @param timestamp The target timestamp to search for in milliseconds since the Unix epoch.
   * @param timeout The maximum duration to wait for the operation to complete.
   * @return The offset of the first message with a timestamp greater than or equal to the target timestamp,
   *         or {@code null} if no such message is found for the partition.
   * @throws PubSubOpTimeoutException If the operation times out while fetching the offset.
   * @throws PubSubClientException If there is an error while attempting to fetch the offset.
   */
  Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp, Duration timeout);

  /**
   * Retrieves the offset of the first message with a timestamp greater than or equal to the target
   * timestamp for the specified PubSub topic-partition. If no such message is found, {@code null}
   * will be returned for the partition.
   *
   * @param pubSubTopicPartition The PubSub topic-partition for which to fetch the offset.
   * @param timestamp The target timestamp to search for in milliseconds since the Unix epoch.
   * @return The offset of the first message with a timestamp greater than or equal to the target timestamp,
   *         or {@code null} if no such message is found for the partition.
   * @throws PubSubOpTimeoutException If the operation times out while fetching the offset.
   * @throws PubSubClientException If there is an error while attempting to fetch the offset.
   */
  Long offsetForTime(PubSubTopicPartition pubSubTopicPartition, long timestamp);

  /**
   * Retrieves the beginning offset for the specified PubSub topic-partition.
   *
   * @param pubSubTopicPartition The PubSub topic-partition for which to fetch the beginning offset.
   * @param timeout The maximum duration to wait for the operation to complete.
   * @return The beginning offset of the specified topic-partition.
   *         If topic-partition exists but has no messages, the offset will be 0.
   * @throws PubSubOpTimeoutException If the operation times out while fetching the beginning offset.
   * @throws PubSubClientException If there is an error while attempting to fetch the beginning offset.
   */
  Long beginningOffset(PubSubTopicPartition pubSubTopicPartition, Duration timeout);

  /**
   * Retrieves the end offsets for a collection of PubSub topic-partitions. The end offset represents
   * the highest offset available in each specified partition, i.e., offset of the last message + 1.
   * If there are no messages in a partition, the end offset will be 0.
   *
   * @param partitions A collection of PubSub topic-partitions for which to fetch the end offsets.
   * @param timeout The maximum duration to wait for the operation to complete.
   * @return A mapping of PubSub topic partitions to their respective end offsets, or an empty map if
   *          the offsets cannot be determined.
   * @throws PubSubOpTimeoutException If the operation times out while fetching the end offsets.
   * @throws PubSubClientException If there is an error while attempting to fetch the end offsets.
   */
  Map<PubSubTopicPartition, Long> endOffsets(Collection<PubSubTopicPartition> partitions, Duration timeout);

  /**
   * Retrieves the end offset for the specified PubSub topic-partition. The end offset represents
   * the highest offset available in each specified partition, i.e., offset of the last message + 1.
   * If there are no messages in a partition, the end offset will be 0.
   *
   * @param pubSubTopicPartition The PubSub topic partition for which to fetch the end offset.
   * @return The end offset of the specified topic partition.
   * @throws PubSubOpTimeoutException If the operation times out while fetching the end offset.
   * @throws PubSubClientException If there is an error while attempting to fetch the end offset.
   */
  Long endOffset(PubSubTopicPartition pubSubTopicPartition);

  /**
   * Retrieves the list of partitions associated with a given Pub-Sub topic.
   *
   * @param pubSubTopic The Pub-Sub topic for which partition information is requested.
   * @return A list of {@link PubSubTopicPartitionInfo} representing the partitions of the topic,
   *         or {@code null} if the topic does not exist.
   */
  List<PubSubTopicPartitionInfo> partitionsFor(PubSubTopic pubSubTopic);
}
