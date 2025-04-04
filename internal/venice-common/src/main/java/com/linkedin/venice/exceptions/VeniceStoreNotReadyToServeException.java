package com.linkedin.venice.exceptions;

import com.linkedin.venice.meta.Store;


/**
 * Custom Exception for Venice Kafka Consumer Related Issues.
 * Currently everything is inherited from the superclass.
 */
public class VeniceStoreNotReadyToServeException extends VeniceException {
  private final String storeName;
  private final int storeVersion;

  public VeniceStoreNotReadyToServeException(String storeName) {
    super("No version of of store " + storeName + " ready to serve");
    this.storeName = storeName;
    this.storeVersion = Store.NON_EXISTING_VERSION;
  }

  public VeniceStoreNotReadyToServeException(String storeName, int storeVersion) {
    super("version " + storeVersion + " of store " + storeName + "not ready to serve");
    this.storeName = storeName;
    this.storeVersion = storeVersion;
  }
}
