package com.linkedin.venice.meta;

/**
 * Interface defined subscription based readonly operations to access stores.
 */
public interface SubscriptionBasedReadOnlyStoreRepository extends ReadOnlyStoreRepository {
  void subscribe(String storeName) throws InterruptedException;

  void unsubscribe(String storeName);
}
