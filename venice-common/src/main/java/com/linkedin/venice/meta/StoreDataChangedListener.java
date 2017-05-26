package com.linkedin.venice.meta;

/**
 * Interface used to register into metadata repository to listen the chage of store data.
 */
public interface StoreDataChangedListener {

  void handleStoreCreated(Store store);

  void handleStoreDeleted(String storeName);

  void handleStoreChanged(Store store);
}
