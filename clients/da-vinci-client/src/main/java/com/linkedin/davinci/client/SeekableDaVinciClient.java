package com.linkedin.davinci.client;

import com.linkedin.davinci.consumer.VeniceChangeCoordinate;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


public interface SeekableDaVinciClient<K, V> extends DaVinciClient<K, V> {
  /**
   * Seek to the specified timestamps for partitions and corresponding timestamps in the map.
   * @param timestamps
   */
  CompletableFuture<Void> seekToTimestamps(Map<Integer, Long> timestamps);

  /**
   * Seek to the specified timestamp for all subscribed partitions.
   * @param timestamp
   */
  CompletableFuture<Void> seekToTimestamp(Long timestamp);

  /**
   * Seek to the begining of the push, for the specified partitions. Same as unsubscribe and subscribe.
   * @param partitions
   * @return
   */
  CompletableFuture<Void> seekToBeginningOfPush(Set<Integer> partitions);

  /**
   * Seek the provided checkpoints for the specified partitions.
   * Note about checkpoints:
   * Checkpoints have the following properties and should be considered:
   *    - Checkpoints are NOT comparable or valid across partitions.
   *    - Checkpoints are NOT comparable or valid across regions
   *    - Checkpoints are NOT comparable across store versions
   *    - It is not possible to determine the number of events between two checkpoints
   *    - It is possible that a checkpoint is no longer on retention. In such case, we will return an exception to the caller.
   * @param checkpoints
   * @return a future which completes when seek has completed for all partitions
   * @throws VeniceException if seek operation failed for any of the partitions
   */
  CompletableFuture<Void> seekToCheckpoint(Set<VeniceChangeCoordinate> checkpoints);

  /**
   * Seek to the end of the last push for all subscribed partitions.
   * @return a future which completes when the operation has succeeded for all partitions.
   * @throws VeniceException if seek operation failed for any of the partitions, or seeking was performed on unsubscribed partitions
   */
  CompletableFuture<Void> seekToTail();

  /**
   * Seek to the end of the last push for the specified partitions.
   * @param partitions the set of partitions to seek with
   * @return a future which completes when the operation has succeeded for all partitions.
   * @throws VeniceException if seek operation failed for any of the partitions, or seeking was performed on unsubscribed partitions
   */
  CompletableFuture<Void> seekToTail(Set<Integer> partitions);
}
