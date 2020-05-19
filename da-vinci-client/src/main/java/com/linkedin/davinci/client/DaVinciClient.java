package com.linkedin.davinci.client;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import com.linkedin.venice.client.store.AvroGenericStoreClient;


/**
 * Da Vinci Client to provide key-value lookups in embedded mode
 * @param <K>
 * @param <V>
 */
public interface DaVinciClient<K, V> extends AvroGenericStoreClient<K, V> {

  /**
   * Ingest the entire data locally.
   *
   * @return a future which completes when the data is ready to serve
   * @throws a VeniceException if subscription failed for any of the partitions
   */
  CompletableFuture<Void> subscribeToAllPartitions();

  /**
   * Ingest a partition locally.
   *
   * @param partitions the set of partition IDs to subscribe to
   * @return a future which completes when the partitions are ready to serve
   * @throws a VeniceException if subscription failed for any of the partitions
   */
  CompletableFuture<Void> subscribe(Set<Integer> partitions);

  /**
   * Stop ingesting a partition locally, and drop its associated local state.
   *
   * @param partitions the set of partition IDs to unsubscribe from
   * @throws a VeniceException if cleanup failed for any of the partitions
   */
  void unsubscribe(Set<Integer> partitions);
}