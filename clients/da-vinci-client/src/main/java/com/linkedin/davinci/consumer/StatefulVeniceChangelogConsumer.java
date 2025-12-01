package com.linkedin.davinci.consumer;

import com.linkedin.venice.pubsub.api.PubSubMessage;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * This interface is meant for users where local state must be built off of the entirety of a Venice data set.
 *
 * This interface provides automatic state management with local checkpointing and efficient data access
 * through local compaction. It eliminates the need for manual checkpoint management and improves restart performance.
 *
 * KEY BENEFITS:
 * - Automatic State Management: No need to manually manage checkpoints.
 *   The client handles all state management automatically.
 * - Efficient Restart: Resumes from the last checkpoint on restart, consuming only new changes since the last
 *   Kafka checkpoint. This reduces recovery time and eliminates the need to re-consume every event from Kafka
 *   on restart.
 * - Local Compaction: All data is compacted locally, providing efficient access to the current state without consuming
 *   duplicate events.
 * - Fast Bootstrap on Fresh Nodes: On fresh nodes without local state, obtains a complete data snapshot from existing
 *   nodes instead of consuming evert Kafka event (requires blob transfer to be enabled).
 *
 * This interface intentionally does not expose seek() operations for simplicity.
 * For more fine-grained control over seeking, see {@link VeniceChangelogConsumer}.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public interface StatefulVeniceChangelogConsumer<K, V> extends AutoCloseable {
  /**
   * Starts the consumer by subscribing to the specified partitions. On restart, the client automatically resumes
   * from the last checkpoint. On fresh start, it begins from the beginning of the topic or leverages blob transfer
   * if available.
   *
   * The returned future completes when there is at least one message available to be polled.
   * Use {@link #isCaughtUp()} to determine when all subscribed partitions have caught up to the latest offset.
   *
   * @param partitions Set of partition IDs to subscribe to. Pass empty set to subscribe to all partitions.
   * @return A future that completes when at least one message is available to poll.
   */
  CompletableFuture<Void> start(Set<Integer> partitions);

  /**
   * Subscribes to every partition for the Venice store. See {@link #start(Set)} for more information.
   */
  CompletableFuture<Void> start();

  void stop() throws Exception;

  /**
   * Polls for the next batch of change events. The first records returned after calling {@link #start(Set)} will be from the
   * local compacted state. Once the local state is fully consumed, subsequent calls will return
   * real-time updates made to the Venice store.
   *
   * Records are returned in batches up to the configured MAX_BUFFER_SIZE. This method will return immediately if:
   * 1. The buffer reaches MAX_BUFFER_SIZE before the timeout expires, OR
   * 2. The timeout is reached
   *
   * NOTE: If the PubSubMessage came from disk (after restart), the following fields will be set to sentinel values
   * since record metadata information is not persisted to reduce disk utilization:
   *  - PubSubMessageTime
   *  - Position
   *
   * @param timeoutInMs Maximum timeout of the poll invocation in milliseconds
   * @return A collection of Venice PubSubMessages containing change events
   */
  Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> poll(long timeoutInMs);

  /**
   * Indicates whether all subscribed partitions have caught up to the latest offset at the time of subscription.
   * Once this becomes true, it will remain true even if the consumer begins to lag later on.
   *
   * This is for determining when the initial bootstrap phase has completed and the consumer has transitioned
   * to consuming real-time events.
   *
   * @return True if all subscribed partitions have caught up to their target offsets.
   */
  boolean isCaughtUp();

  /**
   * Returns the timestamp of the last heartbeat received for each subscribed partition.
   * Heartbeats are messages sent every minute by Venice servers to measure lag.
   *
   * @return a map of partition number to the timestamp, in milliseconds, of the last
   *         heartbeat received for that partition.
   */
  Map<Integer, Long> getLastHeartbeatPerPartition();
}
