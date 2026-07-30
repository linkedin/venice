package com.linkedin.venice.controller;

import com.linkedin.venice.meta.Store;


/**
 * Handles a successful store update consumed by a parent controller.
 */
@FunctionalInterface
public interface StoreUpdateHandler {
  StoreUpdateHandler NO_OP = (clusterName, store) -> {};

  /**
   * @param clusterName the cluster containing the updated store
   * @param store a read-only snapshot of the final store state
   */
  void handleStoreUpdate(String clusterName, Store store);
}
