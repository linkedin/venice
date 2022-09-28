package com.linkedin.venice.client.consumer;

import com.linkedin.venice.annotation.Experimental;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecords;


/**
 * Venice change capture consumer to provide
 * @param <K>
 * @param <V>
 */
@Experimental
public interface VeniceChangelogConsumer<K, V> {
  /**
   * Subscribe a set of partitions to this VeniceChangelogConsumer. The VeniceChangelogConsumer
   * should try to consume messages from all partitions that are subscribed to it.
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
   * @return map of topic partition to records since the last fetch for the subscribed list of topics and partitions
   * @throws a VeniceException if polling operation fails
   */
  ConsumerRecords<K, V> poll(long timeout);
}
