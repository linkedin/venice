package com.linkedin.venice.meta;

/**
 * Interface used to register into metadata repository to listen the change of store data.
 */
public interface StoreDataChangedListener {
  /**
   * Do NOT try to acquire the lock of store repository again in the implementation, otherwise a dead lock issue will
   * happen.
   */
  default void handleStoreCreated(Store store) {
  }

  default void handleStoreDeleted(Store store) {
    handleStoreDeleted(store.getName());
  }

  default void handleStoreDeleted(String storeName) {
  }

  default void handleStoreChanged(Store store) {
  }
}
