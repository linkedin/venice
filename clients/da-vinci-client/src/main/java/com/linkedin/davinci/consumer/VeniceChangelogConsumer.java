package com.linkedin.davinci.consumer;

import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * Venice change capture consumer to provide value change callback.
 *
 * @param <K> The Type for key
 * @param <V> The Type for value
 */
@Experimental
public interface VeniceChangelogConsumer<K, V> {
  /**
   * Subscribe a set of partitions for a store to this VeniceChangelogConsumer. The VeniceChangelogConsumer should try
   * to consume messages from all partitions that are subscribed to it.
   *
   * @param partitions the set of partition to subscribe and consume
   * @return a future which completes when the partitions are ready to be consumed data
   * @throws a VeniceException if subscribe operation failed for any of the partitions
   */
  CompletableFuture<Void> subscribe(Set<Integer> partitions);

  /**
   * Seek to the beginning of the push for a set of partitions.  This is analogous to doing a bootstrap of data for the consumer.
   * This seek will ONLY seek to the beginning of the version which is currently serving data, and the consumer will switch
   * to reading data from a new version (should one get created) once it has read up to the point in the change capture stream
   * that indicates the version swap (which can only occur after consuming all the data in the last push). This instructs
   * the consumer to consume data from the batch push.
   *
   * @param partitions the set of partitions to seek with
   * @return a future which completes when the operation has succeeded for all partitions.
   * @throws VeniceException if seek operation failed for any of the partitions, or seeking was performed on unsubscribed partitions
   */
  CompletableFuture<Void> seekToBeginningOfPush(Set<Integer> partitions);

  /**
   * Seek to the beginning of the push for subscribed partitions. See {@link #seekToBeginningOfPush(Set)} for more information.
   *
   * @return a future which completes when the partitions are ready to be consumed data
   * @throws VeniceException if seek operation failed for any of the partitions.
   */
  CompletableFuture<Void> seekToBeginningOfPush();

  /**
   * Seek to the end of the last push for a given set of partitions. This instructs the consumer to begin consuming events
   * which are transmitted to Venice following the last batch push.
   *
   * @param partitions the set of partitions to seek with
   * @return a future which completes when the operation has succeeded for all partitions.
   * @throws VeniceException if seek operation failed for any of the partitions, or seeking was performed on unsubscribed partitions
   */
  CompletableFuture<Void> seekToEndOfPush(Set<Integer> partitions);

  /**
   * Pause the client on all subscriptions. See {@link #pause(Set)} for more information.
   *
   * @throws VeniceException if operation failed for any of the partitions.
   */
  void pause();

  /**
   * Resume the client on all or subset of partitions this client is subscribed to and has paused.
   *
   * @throws VeniceException if operation failed for any of the partitions.
   */
  void resume(Set<Integer> partitions);

  /**
   * Pause the client on all subscriptions. See {@link #resume(Set)} for more information.
   *
   * @throws VeniceException if operation failed for any of the partitions.
   */
  void resume();

  /**
   * Pause the client on all or subset of partitions this client is subscribed to. Calls to {@link #poll(long)} will not
   * return results from paused partitions until {@link #resume(Set)} is called again later for those partitions.
   *
   * @throws VeniceException if operation failed for any of the partitions.
   */
  void pause(Set<Integer> partitions);

  /**
   * Seek to the end of the push for all subscribed partitions. See {@link #seekToEndOfPush(Set)} for more information.
   *
   * @return a future which completes when the operation has succeeded for all partitions.
   * @throws VeniceException if seek operation failed for any of the partitions.
   */
  CompletableFuture<Void> seekToEndOfPush();

  /**
   * Seek to the end of events which have been transmitted to Venice and start consuming new events. This will ONLY
   * consume events transmitted via nearline and incremental push. It will not read batch push data.
   *
   * @param partitions the set of partitions to seek with
   * @return a future which completes when the operation has succeeded for all partitions.
   * @throws VeniceException if seek operation failed for any of the partitions, or seeking was performed on unsubscribed partitions
   */
  CompletableFuture<Void> seekToTail(Set<Integer> partitions);

  /**
   * Seek to the end of events which have been transmitted to Venice for all subscribed partitions. See {@link #seekToTail(Set)} for more information.
   *
   * @return a future which completes when the operation has succeeded for all partitions.
   * @throws VeniceException if seek operation failed for any of the partitions.
   */
  CompletableFuture<Void> seekToTail();

  /**
   * Seek the provided checkpoints for the specified partitions.
   *
   * Note about checkpoints:
   *
   * Checkpoints have the following properties and should be considered:
   *    -Checkpoints are NOT comparable or valid across partitions.
   *    -Checkpoints are NOT comparable or valid across colos
   *    -Checkpoints are NOT comparable across store versions
   *    -It is not possible to determine the number of events between two checkpoints
   *    -It is possible that a checkpoint is no longer on retention. In such case, we will return an exception to the caller.
   * @param checkpoints
   * @return a future which completes when seek has completed for all partitions
   * @throws VeniceException if seek operation failed for any of the partitions
   */
  CompletableFuture<Void> seekToCheckpoint(Set<VeniceChangeCoordinate> checkpoints);

  /**
   * Subscribe all partitions belonging to a specific store.
   *
   * @return a future which completes when all partitions are ready to be consumed data
   * @throws a VeniceException if subscribe operation failed for any of the partitions
   */
  CompletableFuture<Void> subscribeAll();

  /**
   * Seek to the provided timestamps for the specified partitions based on wall clock time for when this message was
   * processed by Venice and produced to change capture.
   *
   * Note, this API can only be used to seek on nearline data applied to the current serving version in Venice.
   * This will not seek on data transmitted via Batch Push. If the provided timestamp is lower than the earliest
   * timestamp on a given stream, the earliest event will be returned. THIS WILL NOT SEEK TO DATA WHICH WAS APPLIED
   * ON A PREVIOUS VERSION.  You should never seek back in time to a timestamp which is smaller than the current time -
   * rewindTimeInSeconds configured in the hybrid settings for this Venice store.
   *
   * The timestamp passed to this function should be associated to timestamps processed by this interface. The timestamp
   * returned by {@link PubSubMessage.getPubSubMessageTime()} refers to the time when Venice processed the event, and
   * calls to this method will seek based on that sequence of events.  Note: it bears no relation to timestamps provided by
   * upstream producers when writing to Venice where a user may optionally provide a timestamp at time of producing a record.
   *
   * @param timestamps a map keyed by a partition ID, and the timestamp checkpoints to seek for each partition.
   * @return
   * @throws VeniceException if seek operations failed for any of the specified partitions.
   */
  CompletableFuture<Void> seekToTimestamps(Map<Integer, Long> timestamps);

  /**
   * Seek to the specified timestamp for all subscribed partitions. See {@link #seekToTimestamps(Map)} for more information.
   *
   * @return a future which completes when the operation has succeeded for all partitions.
   * @throws VeniceException if seek operation failed for any of the partitions.
   */
  CompletableFuture<Void> seekToTimestamp(Long timestamp);

  /**
   * Stop ingesting messages from a set of partitions for a specific store.
   *
   * @param partitions The set of topic partitions to unsubscribe
   * @throws a VeniceException if unsubscribe operation failed for any of the partitions
   */
  void unsubscribe(Set<Integer> partitions);

  /**
   * Stop ingesting messages from all partitions.
   *
   * @throws a VeniceException if unsubscribe operation failed for any of the partitions
   */
  void unsubscribeAll();

  /**
   * Polling function to get any available messages from the underlying system for all partitions subscribed.
   *
   * @param timeoutInMs The maximum time to block/wait in between two polling requests (must not be greater than
   *        {@link Long#MAX_VALUE} milliseconds)
   * @return a collection of messages since the last fetch for the subscribed list of topic partitions
   * @throws a VeniceException if polling operation fails
   */
  Collection<PubSubMessage<K, ChangeEvent<V>, VeniceChangeCoordinate>> poll(long timeoutInMs);

  /**
   * Release the internal resources.
   */
  void close();
}
