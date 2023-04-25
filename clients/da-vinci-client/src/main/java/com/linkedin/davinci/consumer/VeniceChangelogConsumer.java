package com.linkedin.davinci.consumer;

import com.linkedin.venice.annotation.Experimental;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import java.util.Collection;
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
   * Subscribe all partitions belonging to a specific store.
   *
   * @return a future which completes when all partitions are ready to be consumed data
   * @throws a VeniceException if subscribe operation failed for any of the partitions
   */
  CompletableFuture<Void> subscribeAll();

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
  Collection<PubSubMessage<K, ChangeEvent<V>, Long>> poll(long timeoutInMs);

  /**
   * Release the internal resources.
   */
  void close();
}
