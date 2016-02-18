package com.linkedin.venice.meta;

/**
 * Interface used to register into metadata repository to listen the chage of store data.
 */
public interface StoreDataChangedListener {
    public void handleStoreUpdated(Store store);

    public void handleStoreDeleted(String storeName);
}
