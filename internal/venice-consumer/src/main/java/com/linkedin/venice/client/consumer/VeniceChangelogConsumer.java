package com.linkedin.venice.client.consumer;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * Venice change capture consumer to provide
 * @param <K>
 * @param <V>
 */
public interface VeniceChangelogConsumer<K, V> {
  /**
   * Subscribe a set of partitions to this VeniceChangelogConsumer. The VeniceChangelogConsumer
   * should try to consume messages from all partitions that are subscribed to it.
   *
   * @return a future which completes when the partitions are ready to be consumed data
   * @param partitions the set of partition to subscribe and consume
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
   * Obtain the underlying store name.
   *
   * @return store name
   */
  String getStoreName();

  /**
   * Stop ingesting messages from a set of partitions for this VeniceChangelogConsumer.
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
   * @param timeout The maximum time to block (must not be greater than {@link Long#MAX_VALUE} milliseconds)
   * @throws org.apache.kafka.common.errors.InterruptException if the calling thread is interrupted before or while
   * this function is called
   */
  Map<K, V> poll(long timeout) throws InterruptedException;
}
