package com.linkedin.venice.controller;

import com.linkedin.venice.meta.Store;
import java.util.Set;


/**
 * Handles a successful store update consumed by a parent controller.
 */
@FunctionalInterface
public interface StoreUpdateHandler {
  StoreUpdateHandler NO_OP = new StoreUpdateHandler() {
    @Override
    public void handleStoreUpdate(String clusterName, Store store, Set<String> updatedConfigs) {
    }

    @Override
    public boolean isNoOp() {
      return true;
    }
  };

  /**
   * @param clusterName the cluster containing the updated store
   * @param store a read-only snapshot of the final store state
   * @param updatedConfigs the immutable set of config keys copied from the durable UPDATE_STORE message; the set is
   *        stable across retries of the same admin operation
   */
  void handleStoreUpdate(String clusterName, Store store, Set<String> updatedConfigs);

  /**
   * @return whether callback-specific work, including fetching the final store snapshot, should be skipped
   */
  default boolean isNoOp() {
    return false;
  }
}
