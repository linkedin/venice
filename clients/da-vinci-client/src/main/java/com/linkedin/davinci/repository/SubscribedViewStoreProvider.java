package com.linkedin.davinci.repository;

import java.util.Set;
import javax.annotation.Nonnull;


/**
 * Interface for {@link com.linkedin.venice.meta.SubscriptionBasedReadOnlyStoreRepository} to provide a set of view
 * stores given the parent store.
 */
public interface SubscribedViewStoreProvider {
  /**
   * @param storeName of the Venice store.
   * @return a set of subscribed view store names associated with the provided Venice store.
   */
  @Nonnull
  Set<String> getSubscribedViewStores(String storeName);
}
