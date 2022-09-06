package com.linkedin.davinci.client;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import java.util.Set;
import java.util.concurrent.CompletableFuture;


/**
 * Da Vinci Client to provide key-value lookups in embedded mode
 * @param <K>
 * @param <V>
 */
public interface DaVinciClient<K, V> extends AvroGenericStoreClient<K, V> {
  /**
   * Ingest the entire data (i.e. all partitions) locally.
   *
   * @return a future which completes when the data is ready to serve
   * @throws a VeniceException if subscription failed for any of the partitions
   */
  CompletableFuture<Void> subscribeAll();

  /**
   * Ingest specific partition/partitions locally.
   *
   * @param partitions the set of partition IDs to subscribe to
   * @return a future which completes when the partitions are ready to serve
   * @throws a VeniceException if subscription failed for any of the partitions
   */
  CompletableFuture<Void> subscribe(Set<Integer> partitions);

  /**
   * Stop ingesting all subscribed partition locally, and drop their associated local states/data.
   *
   * If applications intend to keep the states/data for future use, no need to invoke this function before
   * calling {@link com.linkedin.venice.client.store.AvroGenericStoreClient#close()}.
   *
   * @throws a VeniceException if cleanup failed for any of the partitions
   */
  void unsubscribeAll();

  /**
   * Stop ingesting a partition locally, and drop its associated local states/data.
   *
   * If applications intend to keep the states/data for future use, no need to invoke this function before
   * calling {@link com.linkedin.venice.client.store.AvroGenericStoreClient#close()}.
   *
   * @param partitions the set of partition IDs to unsubscribe from
   * @throws a VeniceException if cleanup failed for any of the partitions
   */
  void unsubscribe(Set<Integer> partitions);

  /**
   * Get partition count of a store.
   *
   * @return partition count
   */
  int getPartitionCount();
}
