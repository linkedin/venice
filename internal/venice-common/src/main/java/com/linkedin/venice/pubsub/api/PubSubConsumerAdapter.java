package com.linkedin.venice.pubsub.api;

import static com.linkedin.venice.pubsub.PubSubConstants.PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_DEFAULT_VALUE;

import com.linkedin.venice.pubsub.PubSubConstants;
import com.linkedin.venice.pubsub.PubSubTopicPartitionInfo;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubClientRetriableException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubOpTimeoutException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubTopicDoesNotExistException;
import com.linkedin.venice.pubsub.api.exceptions.PubSubUnsubscribedTopicPartitionException;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;


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
   * Subscribes to a specified topic-partition if it is not already subscribed. If the topic-partition is
   * already subscribed, this method performs no action.
   *
   * The subscription uses the provided {@link PubSubPosition} to determine the starting offset for
   * consumption. If the position is {@link PubSubSymbolicPosition#EARLIEST}, the consumer will seek to the earliest
   * available message. If it is {@link PubSubSymbolicPosition#LATEST}, the consumer will seek to the latest available
   * message. If a custom position is provided, implementations should resolve it to the corresponding offset
   * or position in the underlying pub-sub system.
   *
   * Implementations of this interface should ensure proper validation of the topic-partition existence and
   * manage consumer assignments. This method does not guarantee immediate subscription state changes and may
   * defer them based on implementation details.
   *
   * @param pubSubTopicPartition the topic-partition to subscribe to
   * @param lastReadPubSubPosition the last known position for the topic-partition
   * @throws IllegalArgumentException if lastReadPubSubPosition is null or of an unsupported type
   * @throws PubSubTopicDoesNotExistException if the specified topic does not exist
   */
  void subscribe(@Nonnull PubSubTopicPartition pubSubTopicPartition, @Nonnull PubSubPosition lastReadPubSubPosition);

  /**
   * Subscribes to a specified topic-partition if it is not already subscribed. If the topic-partition is
   * already subscribed, this method performs no action.
   *
   * <p>The subscription uses the provided {@link PubSubPosition} to determine the starting position for
   * consumption. If the position is {@link PubSubSymbolicPosition#EARLIEST}, the consumer will seek to the earliest
   * available message. If it is {@link PubSubSymbolicPosition#LATEST}, the consumer will seek to the latest available
   * message. If a concrete position is provided, implementations should resolve it to a specific offset or internal
   * position based on the underlying pub-sub system.
   *
   * <p>The {@code inclusive} flag determines whether the message at the specified position (if resolvable to an offset
   * or equivalent) should be included in consumption:
   * <ul>
   *   <li>If {@code true}, the consumer should begin from the exact position specified.</li>
   *   <li>If {@code false}, consumption should begin immediately after the specified position.</li>
   * </ul>
   *
   * <p>Implementations should validate the topic-partition's existence and handle any necessary assignment or
   * internal state initialization. The method does not guarantee immediate subscription effect and may defer
   * changes depending on the consumer's execution model.
   *
   * @param pubSubTopicPartition the topic-partition to subscribe to
   * @param position the position from which consumption should begin
   * @param isInclusive whether to include the message at the given position
   * @throws IllegalArgumentException if {@code position} is null or of an unsupported type
   * @throws PubSubTopicDoesNotExistException if the specified topic does not exist
   */
  void subscribe(
      @Nonnull PubSubTopicPartition pubSubTopicPartition,
      @Nonnull PubSubPosition position,
      boolean isInclusive);

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
  Map<PubSubTopicPartition, List<DefaultPubSubMessage>> poll(long timeoutMs);

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
   * @param timeout The maximum duration to wait for the operation to complete.
   * @return The offset of the first message with a timestamp greater than or equal to the target timestamp,
   *         or {@code null} if no such message is found for the partition.
   * @throws PubSubOpTimeoutException If the operation times out while fetching the offset.
   * @throws PubSubClientException If there is an error while attempting to fetch the offset.
   */
  PubSubPosition getPositionByTimestamp(PubSubTopicPartition pubSubTopicPartition, long timestamp, Duration timeout);

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
  PubSubPosition getPositionByTimestamp(PubSubTopicPartition pubSubTopicPartition, long timestamp);

  /**
   * Retrieves the beginning position for the specified PubSub topic-partition.
   *
   * @param pubSubTopicPartition The PubSub topic-partition for which to fetch the beginning offset.
   * @param timeout The maximum duration to wait for the operation to complete.
   * @return The beginning offset of the specified topic-partition.
   *         If topic-partition exists but has no messages, the offset will be 0.
   * @throws PubSubOpTimeoutException If the operation times out while fetching the beginning offset.
   * @throws PubSubClientException If there is an error while attempting to fetch the beginning offset.
   */
  PubSubPosition beginningPosition(PubSubTopicPartition pubSubTopicPartition, Duration timeout);

  default PubSubPosition beginningPosition(PubSubTopicPartition pubSubTopicPartition) {
    return beginningPosition(
        pubSubTopicPartition,
        Duration.ofMillis(PUBSUB_CONSUMER_API_DEFAULT_TIMEOUT_MS_DEFAULT_VALUE));
  }

  Map<PubSubTopicPartition, PubSubPosition> beginningPositions(
      Collection<PubSubTopicPartition> partitions,
      Duration timeout);

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

  Map<PubSubTopicPartition, PubSubPosition> endPositions(Collection<PubSubTopicPartition> partitions, Duration timeout);

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

  PubSubPosition endPosition(PubSubTopicPartition pubSubTopicPartition);

  /**
   * Retrieves the list of partitions associated with a given Pub-Sub topic.
   *
   * @param pubSubTopic The Pub-Sub topic for which partition information is requested.
   * @return A list of {@link PubSubTopicPartitionInfo} representing the partitions of the topic,
   *         or {@code null} if the topic does not exist.
   */
  List<PubSubTopicPartitionInfo> partitionsFor(PubSubTopic pubSubTopic);

  /**
   * Compares two PubSub positions within the specified topic partition.
   *
   * @param partition The topic partition where the comparison is being performed.
   * @param position1 The first PubSub position.
   * @param position2 The second PubSub position.
   * @return A negative value if {@code position1} is behind {@code position2}, zero if equal,
   *         or a positive value if {@code position1} is ahead of {@code position2}.
   */
  long comparePositions(PubSubTopicPartition partition, PubSubPosition position1, PubSubPosition position2);

  /**
   * Computes the relative difference between two {@link PubSubPosition} instances for a given
   * {@link PubSubTopicPartition}, as {@code position1 - position2}.
   * <p>
   * Implementations must resolve symbolic positions such as {@link PubSubSymbolicPosition#EARLIEST}
   * and {@link PubSubSymbolicPosition#LATEST} to concrete positions based on the partition's
   * start and end positions. This ensures that symbolic references can be treated consistently
   * during subtraction.
   *
   * <p>For example:
   * <ul>
   *   <li>If both positions are concrete, the result is the logical offset difference between them.</li>
   *   <li>If {@code position1} is symbolic (e.g., EARLIEST), it must be resolved to the concrete beginning position.</li>
   *   <li>If {@code position2} is symbolic (e.g., LATEST), it must be resolved to the concrete end position.</li>
   * </ul>
   *
   * @param partition The topic partition for which the difference is calculated.
   * @param position1 The first PubSub position (minuend).
   * @param position2 The second PubSub position (subtrahend).
   * @return The signed offset difference between {@code position1} and {@code position2}.
   * @throws IllegalArgumentException if either position is {@code null}, or if symbolic positions cannot be resolved.
   */
  long positionDifference(PubSubTopicPartition partition, PubSubPosition position1, PubSubPosition position2);

  /**
   * Decodes the given type-encoded byte array into a {@link PubSubPosition} for the specified topic partition.
   *
   * @param partition The topic partition this position belongs to.
   * @param positionTypeId The type ID of the position, which indicates how to decode the byte array.
   * @param data The byte array containing the encoded position.
   * @return The decoded {@link PubSubPosition}.
   * @throws IllegalArgumentException if the data cannot be decoded into a valid {@link PubSubPosition}.
   */
  default PubSubPosition decodePosition(PubSubTopicPartition partition, int positionTypeId, byte[] data) {
    return decodePosition(partition, positionTypeId, ByteBuffer.wrap(data));
  }

  /**
   * Decodes the given {@link ByteBuffer} into a {@link PubSubPosition} for the specified topic partition.
   *
   * @param partition The topic partition this position belongs to.
   * @param positionTypeId The type ID of the position, which indicates how to decode the byte buffer.
   * @param buffer The {@link ByteBuffer} containing the encoded position.
   * @return The decoded {@link PubSubPosition}.
   * @throws IllegalArgumentException if the buffer cannot be decoded into a valid {@link PubSubPosition}.
   */
  PubSubPosition decodePosition(PubSubTopicPartition partition, int positionTypeId, ByteBuffer buffer);
}
